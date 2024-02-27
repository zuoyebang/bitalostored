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

package server

import (
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const (
	deleteTaskIntervalMin         = 90
	deleteTaskIntervalDefault     = 180
	deleteTaskQPSThresholdDefault = 20000
)

func (s *Server) RunDeleteExpireDataTask() {
	if !config.GlobalConfig.Bitalos.EnableExpiredDeletion {
		log.Infof("delete expire data task not run, config run_del_expire is false")
		return
	}

	deleteTaskInterval := config.GlobalConfig.Bitalos.ExpiredDeletionInterval * 3
	if deleteTaskInterval%5 != 0 {
		deleteTaskInterval = deleteTaskInterval / 5 * 5
	}
	if deleteTaskInterval < deleteTaskIntervalMin {
		deleteTaskInterval = deleteTaskIntervalMin
	}
	if deleteTaskInterval == 0 {
		deleteTaskInterval = deleteTaskIntervalDefault
	}

	deleteTaskQPSThreshold := config.GlobalConfig.Bitalos.ExpiredDeletionQpsThreshold
	if deleteTaskQPSThreshold == 0 {
		deleteTaskQPSThreshold = deleteTaskQPSThresholdDefault
	}

	isCheckDisable := true
	disableStart := config.GlobalConfig.Bitalos.ExpiredDeletionDisableStartTime
	disableEnd := config.GlobalConfig.Bitalos.ExpiredDeletionDisableEndTime
	if disableStart == 0 && disableEnd == 0 {
		isCheckDisable = false
	}

	log.Infof("delete expire data task start [interval:%d] [maxQps:%d]", deleteTaskInterval, deleteTaskQPSThreshold)

	s.expireWg.Add(1)
	go func() {
		defer s.expireWg.Done()

		var jobId, currentQPS uint64
		interval := time.Duration(deleteTaskInterval)
		ticker := time.NewTicker(interval * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.expireClosedCh:
				log.Info("RunDeleteExpireDataTask receive quit signal")
				return
			case <-ticker.C:
				currentHour := time.Now().Hour()
				if isCheckDisable && disableStart <= currentHour && currentHour <= disableEnd {
					log.Infof("RunDeleteExpireDataTask do nothing disableHour:(%d-%d)", disableStart, disableEnd)
					continue
				}

				currentQPS = s.Info.Stats.QPS.Load()
				if currentQPS >= deleteTaskQPSThreshold {
					log.Infof("RunDeleteExpireDataTask do nothing qps:%d", currentQPS)
					continue
				}

				jobId++
				s.GetDB().ScanDelExpire(jobId)
			}
		}
	}()
}
