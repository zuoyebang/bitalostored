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
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
	"github.com/zuoyebang/bitalostored/dashboard/models"
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
		s.groupsyncStats[gid] = errs
	} else {
		delete(s.groupsyncStats, gid)
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
			if err := s.doLogCompact(gserver.Addr); err != nil {
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
			s.groupsyncStats[g.Id] = errs
		} else {
			delete(s.groupsyncStats, g.Id)
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
			if x.Addr == addr {
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

func (s *DashCore) doLogCompact(addr string) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, time.Second); err != nil {
		log.WarnErrorf(err, "logcompact create redis client to %s failed", addr)
	} else {
		defer c.Close()
		if err := c.LogCompact(); err != nil {
			log.WarnErrorf(err, "logcompact, addr : %s, err : %s", addr, err.Error())
			return err
		}
	}
	return nil
}

func (s *DashCore) groupPromoteServerByRaft(gid int, masterAddr string) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "promote server group id :%d, create redis client to %s failed", gid, masterAddr)
	} else {
		defer c.Close()
		if err := c.PromoteMaster(); err != nil {
			log.WarnErrorf(err, "promote server group id : %d, master : %s, err : %s", gid, masterAddr, err.Error())
			return err
		}
	}

	return nil
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

	if model == 1 {
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
	} else if model == 2 {
		if group.Servers[index].ServerRole != models.ServerOberserNode {
			return errors.Errorf("mount node role is %s, not %s", group.Servers[index].ServerRole, models.ServerOberserNode)
		}
		return s.groupMountRaftObserverNode(gid, masterAddr, raftAddr, nodeId)
	} else if model == 3 {
		return s.groupRemoveRaftNode(ctx, gid, masterAddr, raftAddr, serverAddr, nodeId)
	} else if model == 4 {
		if group.Servers[index].ServerRole != models.ServerWitnessNode {
			return errors.Errorf("mount node role is %s, not %s", group.Servers[index].ServerRole, models.ServerWitnessNode)
		}
		return s.groupMountRaftWitnessNode(gid, masterAddr, raftAddr, nodeId)
	}
	return fmt.Errorf("mount node model not support, [model:%d]", model)
}

func (s *DashCore) groupRemoveRaftNode(ctx *context, gid int, masterAddr, raftAddr, serverAddr string, nodeId int) error {
	if serverAddr == masterAddr && ctx.isGroupInUse(gid) {
		log.Warnf("group id :%d has slot, cant remove this node:%d", gid, nodeId)
		return fmt.Errorf("group has slot")
	}
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "remove raft node server group id :%d, create redis client to %s failed", gid, masterAddr)
	} else {
		defer c.Close()
		if err := c.RemoveRaftNode(nodeId); err != nil {
			log.WarnErrorf(err, "remove raft node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
	}
	return nil
}

func (s *DashCore) groupMountRaftNormalNode(gid int, masterAddr string, raftAddr string, nodeId int) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount raft node server group id :%d, create redis client to %s failed", gid, masterAddr)
	} else {
		defer c.Close()
		if err := c.AddToSlave(raftAddr, nodeId); err != nil {
			log.WarnErrorf(err, "mount raft node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
	}

	return nil
}

func (s *DashCore) groupMountRaftObserverNode(gid int, masterAddr string, raftAddr string, nodeId int) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount observer node server group id :%d, create redis client to %s failed", gid, masterAddr)
	} else {
		defer c.Close()
		if err := c.AddObserver(raftAddr, nodeId); err != nil {
			log.WarnErrorf(err, "mount observer node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
	}
	return nil
}

func (s *DashCore) groupMountRaftWitnessNode(gid int, masterAddr string, raftAddr string, nodeId int) error {
	if c, err := uredis.NewClient(masterAddr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "mount observer node server group id :%d, create redis client to %s failed", gid, masterAddr)
	} else {
		defer c.Close()
		if err := c.AddWitness(raftAddr, nodeId); err != nil {
			log.WarnErrorf(err, "mount observer node server group id : %d, master : %s, raftaddr : %s, err : %s", gid, masterAddr, raftAddr, err.Error())
			return err
		}
	}
	return nil
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
		if memberShip, err := c.GetClusterMemberShip(); err != nil {
			log.WarnErrorf(err, "server group id : %d, master : %s, err : %s", gid, addr, err.Error())
			return nil, err
		} else {
			return memberShip, nil
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

func (s *DashCore) DeRaftGroup(gid int, addr, token string) error {
	if c, err := uredis.NewClient(addr, s.config.ProductAuth, 5*time.Second); err != nil {
		log.WarnErrorf(err, "deraft server group id :%d, create redis client to %s failed", gid, addr)
		return err
	} else {
		defer c.Close()
		if err := c.DeRaft(token); err != nil {
			log.WarnErrorf(err, "deraft server group id : %d, master : %s, err : %s", gid, addr, err.Error())
			return err
		} else {
			return nil
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

func (s *DashCore) EnableReplicaGroupsAll(value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
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
