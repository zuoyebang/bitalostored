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

package raft

import (
	"bytes"
	"time"

	braft "github.com/zuoyebang/bitalostored/raft"
	"github.com/zuoyebang/bitalostored/raft/statemachine"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/marshal/update"
	"github.com/zuoyebang/bitalostored/stored/server"
	"google.golang.org/protobuf/proto"
)

func addPluginStartInitRaft(raft *StartRun) {
	server.AddPlugin(&server.Proc{
		Start: func(s *server.Server) {
			raft.LoadConfig(s)

			raft.registerRaftCommand(s)
			raft.registerIsMasterCF(s)
			raft.registerSyncToSlave(s)

			if !config.GlobalConfig.Plugin.OpenRaft || config.GlobalConfig.CheckIsDegradeSingleNode() {
				s.Info.Cluster.Role = "single"
				s.Info.Cluster.Status = true
			} else {
				node, err := braft.NewNodeHost(raft.Nhc)
				if err != nil {
					log.Error("new host: ", err)
					panic(err)
				}
				raft.Nh = node

				if err := raft.Nh.StartOnDiskCluster(raft.AddrList, raft.Join, func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
					s.Info.Cluster.ClusterId = clusterID
					s.Info.Cluster.CurrentNodeId = nodeID
					return NewDiskKV(clusterID, nodeID, s, raft)
				}, raft.Rc); err != nil {
					log.Error("start cluster: ", err)
					panic(err)
				}
				raft.RaftReady = true
			}

			raft.doRaftClusterStat(s)
		},
		Stop: func(s *server.Server, e interface{}) {
			if raft != nil && raft.Nh != nil {
				raft.StopNodeHost()
			}
		},
	})
}

func addPluginPreparePropose(raft *StartRun) {
	server.AddRaftPlugin(&server.Proc{DoRaftSync: func(c *server.Client, cmd *server.Cmd, key string) error {
		migrate := false
		if b, e := proto.Marshal(&update.ByteSlice{IsMigrate: &migrate, NodeId: &raft.NodeID, Data: c.Data, KeyHash: &c.KeyHash}); e != nil {
			return e
		} else {
			start := time.Now()
			if raft.AsyncPropose {
				if ret, err := raft.Propose(b, raft.RetryTimes); ret != R_SUCCESS {
					c.RespWriter.WriteError(err)
				} else {
					raftSyncCostUs := time.Since(start).Nanoseconds()
					return c.ApplyDB(raftSyncCostUs)
				}
			} else {
				if res, err := raft.SyncPropose(b); err != nil {
					return err
				} else {
					if bytes.Equal(res.Data, UpdateSelfNodeDoing) {
						raftSyncCostNs := time.Since(start).Nanoseconds()
						return c.ApplyDB(raftSyncCostNs)
					} else {
						c.RespWriter.WriteBytes(res.Data)
					}
				}
			}
		}
		return nil
	}})
}
