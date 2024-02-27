// Copyright 2019 The Bitalostored author and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dashcore

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2"

	"github.com/zuoyebang/bitalostored/butils/math2"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

func (s *DashCore) ProcessSlotAction() error {
	for s.IsOnline() {
		var (
			marks = make(map[int]bool)
			plans = make(map[int]bool)
		)
		var accept = func(m *models.SlotMapping) bool {
			if marks[m.GroupId] || marks[m.Action.TargetId] {
				return false
			}
			if plans[m.Id] {
				return false
			}
			return true
		}
		var update = func(m *models.SlotMapping) bool {
			if m.GroupId != 0 {
				marks[m.GroupId] = true
			}
			marks[m.Action.TargetId] = true
			plans[m.Id] = true
			return true
		}
		var parallel = math2.MaxInt(1, 36)
		for parallel > len(plans) {
			_, ok, err := s.SlotActionPrepareFilter(accept, update)
			if err != nil {
				return err
			} else if !ok {
				break
			}
		}
		if len(plans) == 0 {
			return nil
		}
		log.Infof("ProcessSlotAction plans len: %d, plans : %v", len(plans), plans)
		var fut sync2.Future
		for sid, _ := range plans {
			fut.Add()
			go func(sid int) {
				//log.Warnf("slot-[%d] process action", sid)
				err := s.processSlotAction(sid)
				if err != nil {
					status := fmt.Sprintf("Migrate Slot[%04d] [ERROR:%s]", sid, err.Error())
					s.action.progress.status.Store(status)
					log.Error(status)
				} else {
					s.action.progress.status.Store("")
				}
				fut.Done(strconv.Itoa(sid), err)
			}(sid)
		}
		for _, v := range fut.Wait() {
			if v != nil {
				return v.(error)
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return nil
}

func (s *DashCore) processSlotAction(sid int) error {
	begin := time.Now()
	defer func() {
		end := time.Now().Sub(begin)
		log.Infof("slot-[%d] migrate action executor end, cost : %s", sid, butils.FmtDuration(end))
	}()
	for s.IsOnline() {
		if exec, err := s.newSlotActionExecutor(sid); err != nil {
			if errors.Is(err, ErrInitGroupID) {
				return s.SlotActionComplete("", sid)
			}
			return err
		} else if exec == nil {
			time.Sleep(time.Second)
		} else {
			needMirgate, sourceAddr, sourceGroupID, targetGroupID, err := exec()
			if err != nil {
				return err
			}
			log.Infof("slot-[%d] migrate action executor start, [sourceAddr:%s] [sourceGroupID:%d] [targetGroupID:%d]", sid, sourceAddr, sourceGroupID, targetGroupID)

			if !needMirgate {
				status := fmt.Sprintf("Migrate Slot[%04d] [OK]", sid)
				s.action.progress.status.Store(status)
				return s.SlotActionComplete("", sid)
			} else {
				migrate := &models.Migrate{
					SID:           sid,
					SourceGroupID: sourceGroupID,
					TargetGroupID: targetGroupID,
					Status:        nil,
					CreateTime:    time.Now().Format("2006-01-02 15:04:05"),
					UpdateTime:    time.Now().Format("2006-01-02 15:04:05"),
				}
				if err := s.storeUpdateMigrate(migrate); err != nil {
					return err
				}
				s.dirtyMigrateCache(sid)
				if migrateStatus := s.cronMonitorSlotActionComplete(sourceAddr, sid); migrateStatus != nil {
					if migrateStatus.Status == models.MigrateFinshed {
						migrate.Status = migrateStatus
						migrate.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
						if err := s.storeUpdateMigrate(migrate); err != nil {
							return err
						}
						s.dirtyMigrateCache(sid)
						return s.SlotActionComplete(sourceAddr, sid)
					} else {
						time.Sleep(time.Second)
						return s.processSlotAction(sid)
					}
				}
			}
		}
	}
	return nil
}

func (s *DashCore) cronMonitorSlotActionComplete(sourceAddr string, sid int) *models.MigrateStatus {
	retry := 3
	for s.IsOnline() {
		if migrateStatus, err := s.getSlotActionMigrateStatus(sourceAddr, sid); err != nil {
			time.Sleep(time.Second)
			log.Warnf("slot-[%d] migrate action sourceAddr :%s, err : %s", sid, sourceAddr, err.Error())
		} else {
			log.Infof("slot-[%d] migrate action executor sourceAddr :%s, running status : %s", sid, sourceAddr, string(migrateStatus.Encode()))
			if migrateStatus.Status == models.MigrateFinshed {
				return migrateStatus
			} else if migrateStatus.Status == models.MigrateRunning {
				time.Sleep(time.Second)
			} else {
				if retry == 0 {
					//异常错误，结束定时探测，重新发送迁移命令
					return migrateStatus
				}
				retry--
				time.Sleep(time.Second)
			}
		}
	}
	return nil
}
