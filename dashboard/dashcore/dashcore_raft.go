// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/models"

	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
)

func (s *DashCore) crontabCheckMasterByRaft() {
	go func() {
		for {
			s.mu.Lock()
			s.checkMastersByRaft()
			s.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
}

func (s *DashCore) checkMastersByRaft() {
	ctx, err := s.newContext()
	if err != nil {
		return
	}

	if len(ctx.group) == 0 {
		log.Warnf("raft check master err : ctx.group is empty")
		return
	}
	masterGroupMap := make(map[int]string)
	cache := uredis.NewInfoCache(s.config.ProductAuth, time.Second, s.stats.redisp)
	for _, m := range ctx.group {
		addrs := make(map[string]string, len(m.Servers))
		skipDeRaft := false
		var deraftAddr string
		for _, groupServer := range m.Servers {
			if groupServer.ServerRole == models.ServerDeRaftNode {
				skipDeRaft = true
				deraftAddr = groupServer.Addr
				break
			}
			if len(groupServer.ServerRole) == 0 || groupServer.ServerRole == models.ServerMasterSlaveNode {
				addrs[groupServer.Addr] = groupServer.Addr
			}
		}
		if skipDeRaft {
			if err := s.trySwitchGroupMaster(m.Id, deraftAddr, cache); err != nil {
				log.WarnErrorf(err, "start check raft switch group master single failed")
			} else {
				masterGroupMap[m.Id] = deraftAddr
			}
			continue
		}
		if len(addrs) > 0 {
			if master, err := cache.GetRaftGroupMaster(m.Id, addrs); err != nil {
				log.Warnf("checkMastersByRaft GetRaftGroupMaster err : %s", err.Error())
			} else {
				if err := s.trySwitchGroupMaster(m.Id, master, cache); err != nil {
					log.WarnErrorf(err, "start check raft switch group master failed")
				} else {
					masterGroupMap[m.Id] = master
				}
			}
		}
	}
	s.ha.masters = masterGroupMap
}
