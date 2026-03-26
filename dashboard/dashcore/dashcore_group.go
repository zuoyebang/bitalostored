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
	"fmt"
	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2"
	dbclient "github.com/zuoyebang/bitalostored/dashboard/models/db"
	"strconv"
	"strings"
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

const (
	ModelMountRaftNode = 1
	ModelMountObserver = 2
	ModelRemoveNode    = 3
	ModelMountWitness  = 4
)

func (s *DashCore) CreateGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if gid <= 0 || gid > models.MaxGroupId {
		return errors.Errorf("invalid group id = %d, out of range", gid)
	}
	if ctx.group[gid] != nil {
		return errors.Errorf("group-[%d] already exists", gid)
	}
	defer s.dirtyGroupCache(gid)

	g := &models.Group{
		Id:      gid,
		Servers: []*models.GroupServer{},
	}
	return s.storeCreateGroup(g)
}

func (s *DashCore) RemoveGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	if ctx.isGroupInUse(gid) {
		return errors.Errorf("group still inuse, cannot remove")
	}

	defer s.dirtyGroupCache(g.Id)

	return s.storeRemoveGroup(g)
}

func (s *DashCore) ResyncGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	var errs []error
	if errs = s.resyncSlotMappingsByGroupId(ctx, gid); len(errs) > 0 {
		log.Warnf("group-[%d] resync-group failed, errs: %v", g.Id, errs)
		g.SyncFailed = true
	} else {
		g.SyncFailed = false
	}

	defer s.dirtyGroupCache(gid)

	g.OutOfSync = false
	return s.storeUpdateGroup(g)
}

func (s *DashCore) LogCompactGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	for _, gserver := range g.Servers {
		if gserver.ServerRole == models.ServerMasterSlaveNode || gserver.ServerRole == models.ServerOberserNode {
			if err := s.doLogCompact(gserver.Addr, gid); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *DashCore) ResyncGroupAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	var errs []error
	for _, g := range ctx.group {
		if errs = s.resyncSlotMappingsByGroupId(ctx, g.Id); len(errs) > 0 {
			log.Warnf("group-[%d] resync-group failed, errs: %v", g.Id, errs)
			g.SyncFailed = true
		} else {
			g.SyncFailed = false
		}
		defer s.dirtyGroupCache(g.Id)
		g.OutOfSync = false
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *DashCore) GroupAddServer(gid int, serveRole, ct, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid server address")
	}

	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Addr == addr && g.Id == gid {
				return errors.Errorf("server-[%s] already exists", addr)
			}
		}
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers = append(g.Servers, &models.GroupServer{Addr: addr, CloudType: ct, ServerRole: serveRole})
	return s.storeUpdateGroup(g)
}

func (s *DashCore) ChangeServerRole(gid int, serveRole, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groupServerRoleTrans(gid, serveRole, addr)
}

func (s *DashCore) groupServerRoleTrans(gid int, serveRole, addr string) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid server address")
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	for index, group := range g.Servers {
		if group.Addr == addr {
			g.Servers[index].ServerRole = serveRole // models.ServerMasterSlaveNode
			break
		}
	}
	defer s.dirtyGroupCache(g.Id)
	return s.storeUpdateGroup(g)
}

func (s *DashCore) GroupDelServer(gid int, addr string, nodeId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if index == 0 {
		if len(g.Servers) != 1 || ctx.isGroupInUse(g.Id) {
			return errors.Errorf("group-[%d] can't remove master, still in use", g.Id)
		}
	}
	defer s.dirtyGroupCache(g.Id)

	if index != 0 && g.Servers[index].ReplicaGroup {
		g.OutOfSync = true
	}

	var slice = make([]*models.GroupServer, 0, len(g.Servers))
	for i, x := range g.Servers {
		if i != index {
			slice = append(slice, x)
		}
	}
	if len(slice) == 0 {
		g.OutOfSync = false
	}

	g.Servers = slice

	return s.storeUpdateGroup(g)
}

func (s *DashCore) doLogCompact(addr string, gid int) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, time.Second); err != nil {
		log.WarnErrorf(err, "logcompact create redis client to %s failed", addr)
		return err
	} else {
		defer c.Close()
		if err := c.LogCompact(gid); err != nil {
			log.WarnErrorf(err, "logcompact, addr : %s, err : %s", addr, err.Error())
			return err
		}
	}
	return nil
}

func (s *DashCore) groupPromoteServerByRaft(gid int, masterAddr string) error {
	var nodeId string
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "promote server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		nodeId, err = c.GetNodeStatus(gid)
		if err != nil {
			log.WarnErrorf(err, "promote server group id : %d, master : %s, err : %s", gid, masterAddr, err.Error())
			return err
		}
		addrPort := strings.Split(masterAddr, ":")
		port, _ := strconv.Atoi(addrPort[1])
		dbclient.CreateOpsActionLog(addrPort[0], uint(port), s.config.ProductName, dbclient.ServerChangeMaster)
		if err = c.PromoteMasterV7(nodeId, gid); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "promote server group id : %d, nodeId:%s, master : %s, err : %s", gid, nodeId, masterAddr, err.Error())
		}
	}

	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "promote server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err = c.PromoteMasterV6(nodeId, gid); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "promote server group id : %d, nodeId:%s, master : %s, err : %s", gid, nodeId, masterAddr, err.Error())
			return err
		}
	}
}

func (s *DashCore) GroupMountOrOfflineNode(model int, gid int, serverAddr string, raftAddr string, nodeId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	group, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	index, err := ctx.getGroupIndex(group, serverAddr)
	if err != nil {
		return err
	}

	masterAddr := ctx.getGroupMaster(gid)

	if model == ModelMountRaftNode {
		if group.Servers[index].ServerRole == models.ServerOberserNode {
			membership, err := s.GetClusterMembership(gid, masterAddr)
			if err != nil {
				return err
			}
			wasObserver := false
			for _, address := range membership.Info.Observers {
				if address == raftAddr {
					wasObserver = true
					break
				}
			}
			if !wasObserver {
				return errors.Errorf("mount node role is %s, not %s", group.Servers[index].ServerRole, models.ServerMasterSlaveNode)
			}
			err = s.groupMountRaftNormalNode(gid, masterAddr, raftAddr, nodeId)
			if err != nil {
				return err
			}
			return s.groupServerRoleTrans(gid, models.ServerMasterSlaveNode, serverAddr)
		}
		if group.Servers[index].ServerRole != models.ServerMasterSlaveNode {
			return errors.Errorf("mount node role is %s, not %s", group.Servers[index].ServerRole, models.ServerMasterSlaveNode)
		}
		return s.groupMountRaftNormalNode(gid, masterAddr, raftAddr, nodeId)
	} else if model == ModelMountObserver {
		if group.Servers[index].ServerRole != models.ServerOberserNode {
			return errors.Errorf("mount node role is %s, not %s", group.Servers[index].ServerRole, models.ServerOberserNode)
		}
		return s.groupMountRaftObserverNode(gid, masterAddr, raftAddr, nodeId)
	} else if model == ModelRemoveNode {
		return s.groupRemoveRaftNode(ctx, gid, masterAddr, serverAddr, nodeId)
	} else if model == ModelMountWitness {
		if group.Servers[index].ServerRole != models.ServerWitnessNode {
			return errors.Errorf("mount node role is %s, not %s", group.Servers[index].ServerRole, models.ServerWitnessNode)
		}
		return s.groupMountRaftWitnessNode(gid, masterAddr, raftAddr, nodeId)
	}
	return fmt.Errorf("mount node model not support, [model:%d]", model)
}

func (s *DashCore) groupRemoveRaftNode(ctx *context, gid int, masterAddr, serverAddr string, nodeId int) error {
	if serverAddr == masterAddr && ctx.isGroupInUse(gid) {
		log.Warnf("group id :%d has slot, cant remove this node:%d", gid, nodeId)
		return fmt.Errorf("group has slot")
	}
	groupInfo, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	var storeNodeId uint64
	for _, s := range groupInfo.Servers {
		if s.Addr == serverAddr {
			storeNodeId = s.NodeId
		}
	}
	// Case (dead machine): input nodeId = 0 if the machine is dead and isnot to be booted
	if nodeId == 0 {
		nodeId = int(storeNodeId)
	} else {
		if nodeId != int(storeNodeId) {
			log.Warnf("request node id err. request:%d expect:%d", nodeId, storeNodeId)
			return errors.New("nodeId error")
		}
	}
	if nodeId <= 0 {
		return errors.New("node id <= 0 error")
	}

	isLastNode := false
	if len(groupInfo.Servers) == 1 {
		isLastNode = true
	}

	if !isLastNode {
		// Remove the node from raft
		masterClient, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second)
		if err != nil {
			log.WarnErrorf(err, "remove raft node server group id :%d, create redis client to %s failed", gid, masterAddr)
			return err
		}
		defer masterClient.Close()

		if masterClient.GetMajorVersion() >= 7 {
			// v7 master: remove node (maybe v6/v7)
			if err := masterClient.RemoveRaftNodeV7(nodeId, gid); err != nil {
				log.WarnErrorf(err, "remove raft node server group id : %d, master : %s, err : %s", gid, masterAddr, err.Error())
				return err
			}
		} else {
			if err := masterClient.RemoveRaftNodeV6(nodeId, gid); err != nil {
				log.WarnErrorf(err, "remove raft node server group id:%d, master:%s, removed:%d, err:%s", gid, masterAddr, nodeId, err.Error())
				return err
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Send `stopnode`` command to removed node if the version of removed node is >= 7
	var stopClient *uredis.Client
	stopClient, err = uredis.NewClient(serverAddr, s.config.ProductAuth, 5*time.Second)
	if err != nil {
		log.WarnErrorf(err, "create new client(remove node):%s err:%s", serverAddr, err)
		return err
	}
	defer stopClient.Close()

	if stopClient.GetMajorVersion() >= 7 {
		if err = stopClient.StopNodeV7(nodeId, gid); err != nil {
			log.WarnErrorf(err, "stop node(%s) nodeId:%d gid:%d err:%s", serverAddr, nodeId, gid, err)
			return err
		}
	}
	return nil
}

func (s *DashCore) groupMountRaftNormalNode(gid int, masterAddr string, raftAddr string, nodeId int) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount raft node server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err := c.AddToSlaveV7(raftAddr, nodeId, gid); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "mount raft node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
		}
	}

	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount raft node server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err := c.AddToSlaveV6(raftAddr, nodeId, gid); err != nil {
			log.WarnErrorf(err, "mount raft node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
	}
	return nil
}

func (s *DashCore) groupMountRaftObserverNode(gid int, masterAddr string, raftAddr string, nodeId int) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount observer node server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err := c.AddObserverV7(raftAddr, nodeId, gid); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "mount observer node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
		}
	}

	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount observer node server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err := c.AddObserverV6(raftAddr, nodeId, gid); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "mount observer node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
	}
}

func (s *DashCore) groupMountRaftWitnessNode(gid int, masterAddr string, raftAddr string, nodeId int) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount observer node server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err := c.AddWitnessV7(raftAddr, nodeId, gid); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "mount observer node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
		}
	}

	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount observer node server group id :%d, create redis client to %s failed", gid, masterAddr)
		return err
	} else {
		defer c.Close()
		if err := c.AddWitnessV6(raftAddr, nodeId, gid); err != nil {
			log.WarnErrorf(err, "mount observer node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
		return nil
	}
}

func (s *DashCore) GroupPromoteServer(gid int, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.groupPromoteServerByRaft(gid, addr)
}

func (s *DashCore) GetClusterMembership(gid int, addr string) (*uredis.MembershipV2, error) {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "server group id :%d, create redis client to %s failed", gid, addr)
		return nil, err
	} else {
		defer c.Close()
		if memberShip, err := c.GetClusterMemberShipV7(gid); err == nil {
			return memberShip, nil
		} else {
			log.WarnErrorf(err, "server group id : %d, master : %s, err : %s", gid, addr, err.Error())
		}
	}

	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "server group id :%d, create redis client to %s failed", gid, addr)
		return nil, err
	} else {
		defer c.Close()
		if memberShip, err := c.GetClusterMemberShipV6(gid); err == nil {
			return memberShip, nil
		} else {
			log.WarnErrorf(err, "server group id : %d, master : %s, err : %s", gid, addr, err.Error())
			return nil, err
		}
	}
}

func (s *DashCore) GetNodeHostInfo(gid int, addr string) (string, error) {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "server group id :%d, create redis client to %s failed", gid, addr)
		return "", err
	} else {
		defer c.Close()
		if data, err := c.GetNodeHostInfo(); err != nil {
			log.WarnErrorf(err, "server group id : %d, master : %s, err : %s", gid, addr, err.Error())
			return "", err
		} else {
			return data, nil
		}
	}
}

func (s *DashCore) trySwitchGroupMaster(gid int, master string, cache *uredis.InfoCache) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	var index = func() int {
		for i, x := range g.Servers {
			if x.Addr == master {
				return i
			}
		}
		for i, x := range g.Servers {
			rid1 := cache.GetProcessId(master)
			rid2 := cache.GetProcessId(x.Addr)
			if rid1 != "" && rid1 == rid2 {
				return i
			}
		}
		return -1
	}()
	if index == -1 {
		return errors.Errorf("group-[%d] doesn't have server %s with runid = '%s'", g.Id, master, cache.GetProcessId(master))
	}
	if index == 0 {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("group-[%d] will switch master to server[%d] = %s", g.Id, index, g.Servers[index].Addr)

	g.Servers[0], g.Servers[index] = g.Servers[index], g.Servers[0]
	g.OutOfSync = true
	return s.storeUpdateGroup(g)
}

func (s *DashCore) EnableReplicaGroups(gid int, addr string, value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	defer s.dirtyGroupCache(g.Id)

	if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
		g.OutOfSync = true
	}
	if g.Servers[index].ServerRole == models.ServerWitnessNode || g.Servers[index].ServerRole == models.ServerOberserNode {
		return errors.Errorf("group-[%d] addr:[%s] role:[%s] not allow replica", g.Id, addr, g.Servers[index].ServerRole)
	}
	if !value {
		addrPort := strings.Split(addr, ":")
		port, _ := strconv.Atoi(addrPort[1])
		dbclient.CreateOpsActionLog(addrPort[0], uint(port), s.config.ProductName, dbclient.ServerReplica)
	}

	g.Servers[index].ReplicaGroup = value

	return s.storeUpdateGroup(g)
}

func (s *DashCore) DeraftAllGroup(token, cloudType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	errStr := ""
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	groupIds := ctx.getSlotMappingGroupIds()
	var failedGroupIds []int
	for _, groupId := range groupIds {
		g, err := ctx.getGroup(groupId)
		if err != nil {
			failedGroupIds = append(failedGroupIds, groupId)
			errMsg := fmt.Sprintf("GroupId ctx getGroup failed, gid:%v, err: %v", g.Id, err)
			errStr = errStr + errMsg + "\n"
			log.Warn(errMsg)
			continue
		}
		deraftServer := ""
		for _, server := range g.Servers {
			if server.Addr == g.MasterAddr || server.ServerRole == models.ServerDeRaftNode {
				deraftServer = ""
				break
			}
			if server.ServerRole != models.ServerMasterSlaveNode || server.CloudType != cloudType {
				continue
			}
			deraftServer = server.Addr
		}
		if deraftServer != "" {
			err := s.DeRaftGroup(g.Id, deraftServer, token)
			if err != nil {
				failedGroupIds = append(failedGroupIds, groupId)
				errMsg := fmt.Sprintf("GroupId deraft failed, gid:%v, deraftsever:%s, err: %v", g.Id, deraftServer, err)
				errStr = errStr + errMsg + "\n"
				log.Warn(errMsg)
				continue
			}
			if err := s.inverseReplicaGroupsAll(g.Id, deraftServer); err != nil {
				errMsg := fmt.Sprintf("GroupId inverseReplicaGroupsAll failed, gid:%v, deraftsever:%s, err: %v", g.Id, deraftServer, err)
				errStr = errStr + errMsg + "\n"
				log.Warn(errMsg)
			}
		}
	}
	if len(failedGroupIds) > 0 || len(errStr) > 0 {
		return errors.New(errStr)
	}
	return nil
}

func (s *DashCore) ReRaftGroup(gid int, addr, token string, port int) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 1*time.Second); err != nil {
		log.WarnErrorf(err, "reraft server group id :%d, create redis client to %s failed", gid, addr)
		return err
	} else {
		defer c.Close()
		if err := c.ReRaft(token, port); err != nil {
			log.WarnErrorf(err, "reraft server group id : %d, addr : %s, err : %s", gid, addr, err.Error())
			return err
		} else {
			return nil
		}
	}
}

func (s *DashCore) Compact(addr, dbType string) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 1*time.Second); err != nil {
		log.WarnErrorf(err, "compact server create redis client to %s failed", addr)
		return err
	} else {
		defer c.Close()
		if err := c.Compact(dbType); err != nil {
			log.WarnErrorf(err, "compact server addr : %s, err : %s", addr, err.Error())
			return err
		} else {
			return nil
		}
	}
}

func (s *DashCore) AutoCompact(addr string, acSwitch int) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 1*time.Second); err != nil {
		log.WarnErrorf(err, "compact server create redis client to %s failed", addr)
		return err
	} else {
		defer c.Close()
		if err := c.AutoCompact(acSwitch); err != nil {
			log.WarnErrorf(err, "compact server addr : %s, err : %s", addr, err.Error())
			return err
		} else {
			return nil
		}
	}
}

func (s *DashCore) DeRaftGroup(gid int, addr, token string) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "deraft server group id :%d, create redis client to %s failed", gid, addr)
		return err
	} else {
		defer c.Close()
		if err := c.DeRaft(gid, token); err == nil {
			return nil
		} else {
			log.WarnErrorf(err, "deraft server group id : %d, master : %s, err : %s", gid, addr, err.Error())
			return err
		}
	}
}

func (s *DashCore) InverseReplicaGroupsAll(gid int, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inverseReplicaGroupsAll(gid, addr)
}

func (s *DashCore) inverseReplicaGroupsAll(gid int, addr string) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	defer s.dirtyGroupCache(g.Id)

	if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
		g.OutOfSync = true
	}
	if g.Servers[index].ServerRole == models.ServerWitnessNode || g.Servers[index].ServerRole == models.ServerOberserNode {
		return errors.Errorf("group-[%d] addr:[%s] role:[%s] not allow replica", g.Id, addr, g.Servers[index].ServerRole)
	}

	for i, _ := range g.Servers {
		if index != i {
			g.Servers[i].ReplicaGroup = false
			continue
		}
		g.Servers[index].ServerRole = models.ServerDeRaftNode
		g.Servers[index].ReplicaGroup = true
	}
	return s.storeUpdateGroup(g)
}

func (s *DashCore) EnableReplicaGroupsAll(value bool, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if token != s.model.Token {
		return errors.Errorf("token invalid")
	}

	for _, g := range ctx.group {
		if g.Promoting.State != models.ActionNothing {
			return errors.Errorf("group-[%d] is promoting", g.Id)
		}
		defer s.dirtyGroupCache(g.Id)

		var dirty bool
		for _, x := range g.Servers {
			if x.ServerRole == models.ServerWitnessNode || x.ServerRole == models.ServerOberserNode {
				continue
			}
			if x.ReplicaGroup != value {
				x.ReplicaGroup = value
				dirty = true
				if !value {
					addrPort := strings.Split(x.Addr, ":")
					port, _ := strconv.Atoi(addrPort[1])
					dbclient.CreateOpsActionLog(addrPort[0], uint(port), s.config.ProductName, dbclient.ServerReplica)
				}
			}
		}
		if !dirty {
			continue
		}
		if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
			g.OutOfSync = true
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *DashCore) EnableAutoCompactAll(value int) []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := make([]error, 0)
	ctx, err := s.newContext()
	if err != nil {
		errs = append(errs, err)
		return errs
	}

	var fut sync2.Future
	for _, g := range ctx.group {
		if g.Promoting.State != models.ActionNothing {
			errs = append(errs, errors.Errorf("group-[%d] is promoting", g.Id))
			return errs
		}
		for _, x := range g.Servers {
			if x.ServerRole == models.ServerWitnessNode {
				continue
			}
			fut.Add()
			go func(addr string, value int) {
				if c, err := uredis.NewClient(addr, s.config.ProductAuth, 1*time.Second); err != nil {
					fut.Done(addr, errors.Errorf("create redis client to %s failed", addr))
				} else {
					defer c.Close()
					if err := c.AutoCompact(value); err != nil {
						fut.Done(addr, errors.Errorf("auto compact server addr : %s, err : %s", addr, err.Error()))
					} else {
						fut.Done(addr, nil)
					}
				}
			}(x.Addr, value)
		}
	}
	for _, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

func (s *DashCore) SyncCreateAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionPending {
		return errors.Errorf("server-[%s] action already exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = ctx.maxSyncActionIndex() + 1
	g.Servers[index].Action.State = models.ActionPending
	return s.storeUpdateGroup(g)
}

func (s *DashCore) SyncRemoveAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionNothing {
		return errors.Errorf("server-[%s] action doesn't exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionNothing
	return s.storeUpdateGroup(g)
}
