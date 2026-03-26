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
	"github.com/zuoyebang/bitalostored/dashboard/models"
	dbclient "github.com/zuoyebang/bitalostored/dashboard/models/db"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
)

func (s *DashCore) crontabCheckMasterByRaft() {
	go func() {
		for {
			if dbclient.IsAllowUpdate() {
				s.mu.Lock()
				s.checkMastersByRaft()
				s.mu.Unlock()
			}
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
			// if len(groupServer.ServerRole) == 0 || groupServer.ServerRole == models.ServerMasterSlaveNode {
			addrs[groupServer.Addr] = groupServer.Addr
			// }
		}
		if skipDeRaft {
			if err := s.trySwitchGroupMaster(m.Id, deraftAddr, cache); err != nil {
				log.WarnErrorf(err, "start check raft switch group master single failed")
			}
			continue
		}
		if len(addrs) > 0 {
			var nodeList map[string]*uredis.NodeInfo
			var master string
			var err error
			if master, nodeList, err = cache.GetRaftGroupInfo(m.Id, addrs); err != nil {
				log.Warnf("checkMastersByRaft GetRaftGroupMaster err : %s", err.Error())
			} else {
				if err := s.trySwitchGroupMaster(m.Id, master, cache); err != nil {
					log.WarnErrorf(err, "start check raft switch group master failed")
				}
			}
			for _, groupServer := range m.Servers {
				if v, ok := nodeList[groupServer.Addr]; ok && v != nil {
					if groupServer.NodeId == 0 && len(v.CurrentNodeId) > 0 {
						nodeId, _ := strconv.Atoi(v.CurrentNodeId)
						if nodeId > 0 {
							groupServer.NodeId = uint64(nodeId)
						}
					}
				}
			}
		}
	}
}
