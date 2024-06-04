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
	"net"
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/butils/math2"
	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

const MaxSlotNum = models.MaxSlotNum

type context struct {
	slots   []*models.SlotMapping
	group   map[int]*models.Group
	pconfig map[string]*models.Pconfig
	proxy   map[string]*models.Proxy
	migrate map[int]*models.Migrate

	hosts struct {
		sync.Mutex
		m map[string]net.IP
	}
}

func (ctx *context) getSlotMappingGroupIds() []int {
	groupIdMap := make(map[int]bool, 0)
	for _, m := range ctx.slots {
		groupIdMap[m.GroupId] = true
	}
	var groupIds []int
	for groupId := range groupIdMap {
		groupIds = append(groupIds, groupId)
	}
	return groupIds
}

func (ctx *context) getSlotMapping(sid int) (*models.SlotMapping, error) {
	if len(ctx.slots) != MaxSlotNum {
		return nil, errors.Errorf("invalid number of slots = %d/%d", len(ctx.slots), MaxSlotNum)
	}
	if sid >= 0 && sid < MaxSlotNum {
		return ctx.slots[sid], nil
	}
	return nil, errors.Errorf("slot-[%d] doesn't exist", sid)
}

func (ctx *context) getSlotMappingsByGroupId(gid int) []*models.SlotMapping {
	var slots []*models.SlotMapping
	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			slots = append(slots, m)
		}
	}
	return slots
}

func (ctx *context) maxSlotActionIndex() (maxIndex int) {
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			maxIndex = math2.MaxInt(maxIndex, m.Action.Index)
		}
	}
	return maxIndex
}

func (ctx *context) isSlotLocked(m *models.SlotMapping) bool {
	switch m.Action.State {
	case models.ActionNothing, models.ActionPending:
		return ctx.isGroupLocked(m.GroupId)
	case models.ActionPreparing:
		return ctx.isGroupLocked(m.GroupId)
	case models.ActionPrepared:
		return true
	case models.ActionMigrating:
		return ctx.isGroupLocked(m.GroupId) || ctx.isGroupLocked(m.Action.TargetId)
	case models.ActionFinished:
		return ctx.isGroupLocked(m.Action.TargetId)
	default:
		log.Panicf("slot-[%d] action state is invalid:\n%s", m.Id, m.Encode())
	}
	return false
}

func (ctx *context) toSlot(m *models.SlotMapping, p *models.Proxy) *models.Slot {
	slot := &models.Slot{
		Id:     m.Id,
		Locked: ctx.isSlotLocked(m),
	}

	switch m.Action.State {
	case models.ActionNothing, models.ActionPending:
		slot.MasterAddr = ctx.getGroupMaster(m.GroupId)
		slot.MasterAddrGroupId = m.GroupId
		ctx.toCloudServersLocalAndBackup(slot, m.GroupId, p)
	case models.ActionPreparing:
		slot.MasterAddr = ctx.getGroupMaster(m.GroupId)
		slot.MasterAddrGroupId = m.GroupId
		ctx.toCloudServersLocalAndBackup(slot, m.GroupId, p)
	case models.ActionPrepared:
		fallthrough
	case models.ActionMigrating:
		slot.MasterAddr = ctx.getGroupMaster(m.GroupId)
		slot.MasterAddrGroupId = m.GroupId
		ctx.toCloudServersLocalAndBackup(slot, m.GroupId, p)
	case models.ActionFinished:
		slot.MasterAddr = ctx.getGroupMaster(m.Action.TargetId)
		slot.MasterAddrGroupId = m.Action.TargetId
		ctx.toCloudServersLocalAndBackup(slot, m.Action.TargetId, p)
	default:
		log.Panicf("slot-[%d] action state is invalid:\n%s", m.Id, m.Encode())
	}
	return slot
}

func (ctx *context) lookupIPAddr(addr string) net.IP {
	ctx.hosts.Lock()
	defer ctx.hosts.Unlock()
	ip, ok := ctx.hosts.m[addr]
	if !ok {
		if tcpAddr := butils.ResolveTCPAddrTimeout(addr, 50*time.Millisecond); tcpAddr != nil {
			ctx.hosts.m[addr] = tcpAddr.IP
			return tcpAddr.IP
		} else {
			ctx.hosts.m[addr] = nil
			return nil
		}
	}
	return ip
}

func (ctx *context) toCloudServersLocalAndBackup(slot *models.Slot, gid int, p *models.Proxy) {
	g := ctx.group[gid]
	if g == nil {
		return
	}
	var dc string

	if p != nil {
		dc = p.CloudType
	}

	isLocalCloud := func(s *models.GroupServer) bool {
		if dc == s.CloudType {
			return true
		}
		return false
	}

	localCloudIps := make([]string, 0, 2)
	backupCloudIps := make([]string, 0, 2)
	witnessIps := make([]string, 0, 3)
	groupServersCloudMap := make(map[string]string, 4)

	for _, s := range g.Servers {
		if s.ReplicaGroup {
			if isLocalCloud(s) {
				localCloudIps = append(localCloudIps, s.Addr)
			} else {
				backupCloudIps = append(backupCloudIps, s.Addr)
			}
			groupServersCloudMap[s.Addr] = s.CloudType
		} else {
			if s.ServerRole == models.ServerWitnessNode {
				witnessIps = append(witnessIps, s.Addr)
			}
		}
	}
	slot.LocalCloudServers = localCloudIps
	slot.BackupCloudServers = backupCloudIps
	slot.WitnessServers = witnessIps
	slot.GroupServersCloudMap = groupServersCloudMap
	slot.GroupServersStats = nil
}

func (ctx *context) toSlotSlice(slots []*models.SlotMapping, p *models.Proxy) []*models.Slot {
	var slice = make([]*models.Slot, len(slots))
	for i, m := range slots {
		slice[i] = ctx.toSlot(m, p)
	}
	return slice
}

func (ctx *context) toPconfigSlice(pconfigs map[string]*models.Pconfig) []*models.Pconfig {
	var slice = make([]*models.Pconfig, len(pconfigs))
	index := 0
	for _, m := range pconfigs {
		slice[index] = m
		index++
	}
	return slice
}

func (ctx *context) getGroup(gid int) (*models.Group, error) {
	if g := ctx.group[gid]; g != nil {
		return g, nil
	}
	return nil, errors.Errorf("group-[%d] doesn't exist", gid)
}

func (ctx *context) getGroupIndex(g *models.Group, addr string) (int, error) {
	for i, x := range g.Servers {
		if x.Addr == addr {
			return i, nil
		}
	}
	return -1, errors.Errorf("group-[%d] doesn't have server-[%s]", g.Id, addr)
}

func (ctx *context) getGroupByServer(addr string) (*models.Group, int, error) {
	for _, g := range ctx.group {
		for i, x := range g.Servers {
			if x.Addr == addr {
				return g, i, nil
			}
		}
	}
	return nil, -1, errors.Errorf("server-[%s] doesn't exist", addr)
}

func (ctx *context) maxSyncActionIndex() (maxIndex int) {
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Action.State == models.ActionPending {
				maxIndex = math2.MaxInt(maxIndex, x.Action.Index)
			}
		}
	}
	return maxIndex
}

func (ctx *context) minSyncActionIndex() string {
	var d *models.GroupServer
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Action.State == models.ActionPending {
				if d == nil || x.Action.Index < d.Action.Index {
					d = x
				}
			}
		}
	}
	if d == nil {
		return ""
	}
	return d.Addr
}

func (ctx *context) getGroupMaster(gid int) string {
	if g := ctx.group[gid]; g != nil && len(g.Servers) != 0 {
		return g.Servers[0].Addr
	}
	return ""
}

func (ctx *context) getGroupMasters() map[int]string {
	var masters = make(map[int]string)
	for _, g := range ctx.group {
		if len(g.Servers) != 0 {
			masters[g.Id] = g.Servers[0].Addr
		}
	}
	return masters
}

func (ctx *context) getGroupIds() map[int]bool {
	var groups = make(map[int]bool)
	for _, g := range ctx.group {
		groups[g.Id] = true
	}
	return groups
}

func (ctx *context) isGroupInUse(gid int) bool {
	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			return true
		}
	}
	return false
}

func (ctx *context) isGroupLocked(gid int) bool {
	if g := ctx.group[gid]; g != nil {
		switch g.Promoting.State {
		case models.ActionNothing:
			return false
		case models.ActionPreparing:
			return false
		case models.ActionPrepared:
			return true
		case models.ActionFinished:
			return false
		default:
			log.Panicf("invalid state of group-[%d] = %s", g.Id, g.Encode())
		}
	}
	return false
}

func (ctx *context) isGroupPromoting(gid int) bool {
	if g := ctx.group[gid]; g != nil {
		return g.Promoting.State != models.ActionNothing
	}
	return false
}

func (ctx *context) getProxy(token string) (*models.Proxy, error) {
	if p := ctx.proxy[token]; p != nil {
		return p, nil
	}
	return nil, errors.Errorf("proxy-[%s] doesn't exist", token)
}

func (ctx *context) maxProxyId() (maxId int) {
	for _, p := range ctx.proxy {
		maxId = math2.MaxInt(maxId, p.Id)
	}
	return maxId
}
