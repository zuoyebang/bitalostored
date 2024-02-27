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

package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	braft "github.com/zuoyebang/bitalostored/raft"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/marshal/update"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/golang/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
)

func (p *StartRun) AddNode(nodeId uint64, address string, retryTime int) (RetType, error) {
	return p.addRaftNode(nodeId, address, retryTime, p.Nh.RequestAddNode)
}

func (p *StartRun) DelNode(nodeId uint64, retryTime int) (RetType, error) {
	if !p.RaftReady {
		return R_NIL_POINTER, errn.ErrRaftNotReady
	}

	if retryTime < 1 {
		return R_PARA_ERR, errors.New("retryTime is too small")
	}

	var n int = 0
	return p.doDelNode(nodeId, &n, retryTime)
}

func (p *StartRun) doDelNode(nodeId uint64, n *int, max int) (RetType, error) {
	if *n >= max {
		return R_RETRY_EXHAUST, errors.New("the number of retries has been reached")
	}

	*n = *n + 1

	rs, err := p.Nh.RequestDeleteNode(p.Rc.ClusterID, nodeId, 0, p.TimeOut)
	if nil != err {
		return R_ERROR, err
	}

	s := <-rs.AppliedC()
	if s.Timeout() || s.Dropped() {
		p.doDelNode(nodeId, n, max)
	} else if s.Completed() {
		rs.Release()
	} else if s.Terminated() {
		return R_SHUT_DOWN, errors.New("should be shutdown")
	} else if s.Rejected() {
		return R_REJECTED, errors.New("rejected")
	} else if s.Aborted() {
		return R_ABORTED, errors.New("aborted")
	} else {
		return R_UNKNOWN_ERROR, errors.New("unknown err")
	}

	return R_SUCCESS, nil
}

func (p *StartRun) GetNodeHostInfo() (string, RetType, error) {
	if !p.RaftReady {
		return "", R_NIL_POINTER, errn.ErrRaftNotReady
	}
	var opt braft.NodeHostInfoOption
	pRet := p.Nh.GetNodeHostInfo(opt)
	if nil == pRet {
		return "", R_RET_ERROR, errors.New("return nil pointer")
	}

	var out NodeHostInfoV2
	out.Info = *pRet
	data, err := jsoniter.Marshal(out)
	if nil != err {
		return "", R_JOSON_ERR, err
	}
	output := string(data)
	return output, R_SUCCESS, nil
}

func (p *StartRun) GetClusterMembership() (string, RetType, error) {
	if !p.RaftReady {
		return "", R_NIL_POINTER, errn.ErrRaftNotReady
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.TimeOut)
	defer cancel()

	membership, err := p.Nh.SyncGetClusterMembership(ctx, p.Rc.ClusterID)
	if err != nil {
		return "", R_RET_ERROR, err
	}

	mv2 := &MembershipV2{}
	mv2.InitByDragonboatMembership(membership)

	output, err := mv2.Marshal()
	if err != nil {
		return "", R_JOSON_ERR, err
	}

	return output, R_SUCCESS, nil
}

func (p *StartRun) LeaderTransfer(targetNodeID uint64) (RetType, error) {
	if !p.RaftReady {
		return R_NIL_POINTER, errn.ErrRaftNotReady
	}

	err := p.Nh.RequestLeaderTransfer(p.Rc.ClusterID, targetNodeID)
	if nil != err {
		return R_ERROR, err
	}
	return R_SUCCESS, nil
}

func (p *StartRun) GetLeaderId() (uint64, RetType, error) {
	if !p.RaftReady {
		return 0, R_NIL_POINTER, errn.ErrRaftNotReady
	}

	id, ok, err := p.Nh.GetLeaderID(p.Rc.ClusterID)
	if ok {
		return id, R_SUCCESS, nil
	}
	return 0, R_ERROR, err
}

func (p *StartRun) RemoveData(nNodeID uint64) (RetType, error) {
	if !p.RaftReady {
		return R_NIL_POINTER, errn.ErrRaftNotReady
	}
	err := p.Nh.RemoveData(p.Rc.ClusterID, nNodeID)
	if nil != err {
		return R_ERROR, err
	}

	return R_SUCCESS, nil
}

func (p *StartRun) GetOK() (bool, RetType, error) {
	if !p.RaftReady {
		return false, R_NIL_POINTER, errn.ErrRaftNotReady
	}

	return true, R_SUCCESS, nil
}

func (p *StartRun) FullSync() error {
	if !p.RaftReady {
		return errn.ErrRaftNotReady
	}
	return errors.New("not implement")
}

func (p *StartRun) StatInfo() (string, RetType, error) {
	if !p.RaftReady {
		return "", R_NIL_POINTER, errn.ErrRaftNotReady
	}

	return "", R_SUCCESS, nil
}

func (p *StartRun) StopNodeHost() (RetType, error) {
	if !p.RaftReady {
		return R_NIL_POINTER, errn.ErrRaftNotReady
	}
	p.RaftReady = false
	p.Nh.Close()
	p.Nh = nil
	return R_SUCCESS, nil
}

func (p *StartRun) AddObserver(nodeId uint64, address string) (RetType, error) {
	return p.addRaftNode(nodeId, address, p.RetryTimes, p.Nh.RequestAddNonVoting)
}

func (p *StartRun) AddWitness(nodeId uint64, address string) (RetType, error) {
	return p.addRaftNode(nodeId, address, p.RetryTimes, p.Nh.RequestAddWitness)
}

func (p *StartRun) doRaftClusterStat(s *server.Server) {
	raftInstance.ClusterStatOnce.Do(func() {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("raft cluster stat. err:%s", string(debug.Stack()))
				}
			}()

			buf := bytes.NewBuffer(make([]byte, 0, 128))
			for {
				time.Sleep(8 * time.Second)
				if !p.RaftReady {
					if config.GlobalConfig.Server.DegradeSingleNode {
						s.Info.Cluster.Role = "single"
						s.Info.Cluster.Status = true
					} else {
						s.Info.Cluster.Status = false
					}
					s.Info.Cluster.UpdateCache()
					continue
				}
				if p == nil || p.Nh == nil {
					return
				}

				if p.queue != nil {
					s.Info.Stats.QueueLen = p.queue.QLength()
					s.Info.Stats.UpdateCache()
				}

				var opt braft.NodeHostInfoOption
				if res := p.Nh.GetNodeHostInfo(opt); res != nil && len(res.ClusterInfoList) == 1 {
					s.Info.Cluster.Status = GetClusterNodeOK(config.GlobalConfig.RaftCluster.ClusterId)
					for _, clusterInfo := range res.ClusterInfoList {
						if clusterInfo.IsLeader {
							s.Info.Cluster.Role = "master"
						} else if clusterInfo.IsNonVoting {
							s.Info.Cluster.Role = "observer"
							p.IsObserver = true
						} else if clusterInfo.IsWitness {
							p.IsWitness = true
							s.Info.Cluster.Role = "witness"
						} else {
							s.Info.Cluster.Role = "slave"
						}

						if config.GlobalConfig.RaftCluster.IsObserver {
							s.Info.Cluster.StartModel = server.M_OBSERVER
						}

						if s.IsWitness {
							s.Info.Cluster.StartModel = server.M_WITNESS
						}

						if (p.IsObserver && !clusterInfo.IsNonVoting) || config.GlobalConfig.RaftCluster.Join {
							_ = config.GlobalConfig.ResetConfig(config.GlobalConfig.Server.ConfigFile)
							p.IsObserver = config.GlobalConfig.RaftCluster.IsObserver
							s.Info.Cluster.StartModel = server.M_NORMAL
						}

						s.Info.Cluster.ClusterId = clusterInfo.ClusterID
						s.Info.Cluster.CurrentNodeId = clusterInfo.NodeID
						s.Info.Cluster.RaftAddress = res.RaftAddress

						if leaderNodeId, ok, err := p.Nh.GetLeaderID(clusterInfo.ClusterID); ok && err == nil {
							s.Info.Cluster.LeaderNodeId = leaderNodeId
							s.Info.Cluster.LeaderAddress = clusterInfo.Nodes[leaderNodeId]
						}
						nodes := make([]string, 0, len(clusterInfo.Nodes))
						for i, _ := range clusterInfo.Nodes {
							nodes = append(nodes, strconv.FormatInt(int64(i), 10))
						}

						sort.Strings(nodes)
						s.Info.Cluster.ClusterNodes = strings.Join(nodes, ",")

						for _, index := range nodes {
							if node_id, err := strconv.ParseInt(index, 10, 64); err == nil {
								_, _ = fmt.Fprintf(buf, "node_%d:%s,state=online,node_id=%d\n", node_id, clusterInfo.Nodes[uint64(node_id)], node_id)
							}
						}
						s.Info.Cluster.ClusterNodesList = buf.String()
						buf.Reset()
					}
				} else {
					s.Info.Cluster.Status = false
				}
				s.Info.Cluster.UpdateCache()
			}
		}()
	})
}

func (p *StartRun) registerRaftCommand(s *server.Server) {
	server.AddCommand(map[string]*server.Cmd{
		ADD:                  {NArg: 2, Handler: func(c *server.Client) error { return addRaftClusterNode(p, c) }},
		ADD_OBSERVER:         {NArg: 2, Handler: func(c *server.Client) error { return addObserver(p, c) }},
		ADD_WITNESS:          {NArg: 2, Handler: func(c *server.Client) error { return addWitness(p, c) }},
		REMOVE:               {NArg: 1, Handler: func(c *server.Client) error { return removeRaftClusterNode(p, c) }},
		TRANSFER:             {NArg: 1, Handler: func(c *server.Client) error { return transferRaftClusterNode(p, c) }},
		GET_LEADER:           {NArg: 0, Handler: func(c *server.Client) error { return getLeaderFrmRaftCluster(p, c) }},
		GET_NODEHOST_INFO:    {NArg: 0, Handler: func(c *server.Client) error { return getNodeHostInfo(p, c) }},
		GET_CLUSTER_MEM_SHIP: {NArg: 0, Handler: func(c *server.Client) error { return getClusterMemberShip(p, c) }},
		REMOVE_DATA:          {NArg: 1, Handler: func(c *server.Client) error { return removeRaftNodeData(p, c) }},
		OK:                   {NArg: 0, Handler: func(c *server.Client) error { return okNodeHost(p, c) }},
		FULLSYNC:             {NArg: 0, Handler: func(c *server.Client) error { return fullSync(p, c) }},
		STAT_INFO:            {NArg: 0, Handler: func(c *server.Client) error { return statInfo(p, c) }},
		DERAFT:               {NArg: 0, Handler: func(c *server.Client) error { return deraft(s, p, c) }},
		RERAFT:               {NArg: 0, Handler: func(c *server.Client) error { return reRaft(s, p, c) }},
		LOGCOMPACT:           {NArg: 0, Handler: func(c *server.Client) error { return logCompact(p, c) }},
	})
}

func (p *StartRun) registerIsMasterCF(s *server.Server) {
	s.IsMaster = func() bool {
		if !config.GlobalConfig.Plugin.OpenRaft || config.GlobalConfig.CheckIsDegradeSingleNode() {
			return true
		}
		if s.Info.Cluster.LeaderNodeId == p.NodeID {
			return true
		}
		return false
	}
}

func (p *StartRun) registerSyncToSlave(s *server.Server) {
	s.MigrateDelToSlave = func(keyHash uint32, data [][]byte) error {
		if !config.GlobalConfig.Plugin.OpenRaft || config.GlobalConfig.CheckIsDegradeSingleNode() {
			return nil
		}

		migrate := true
		message := &update.ByteSlice{
			IsMigrate: &migrate,
			KeyHash:   &keyHash,
			NodeId:    &p.NodeID,
			Data:      data,
		}
		if b, err := proto.Marshal(message); err != nil {
			return err
		} else if _, err = p.SyncPropose(b); err != nil {
			return err
		}
		return nil
	}
}
