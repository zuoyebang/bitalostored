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
	"encoding/json"
	"fmt"
	"sort"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/models"

	rbtree "github.com/emirpasic/gods/trees/redblacktree"
)

func (s *DashCore) SlotCreateAction(sid int, gid int) error {
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
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", gid)
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State != models.ActionNothing {
		return errors.Errorf("slot-[%d] action already exists", sid)
	}
	if m.GroupId == gid {
		return errors.Errorf("slot-[%d] already in group-[%d]", sid, gid)
	}
	defer s.dirtySlotsCache(m.Id)

	m.Action.State = models.ActionPending
	m.Action.Index = ctx.maxSlotActionIndex() + 1
	m.Action.TargetId = g.Id
	return s.storeUpdateSlotMapping(m)
}

func (s *DashCore) SlotCreateActionSome(groupFrom, groupTo int, numSlots int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(groupTo)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for _, m := range ctx.slots {
		if len(pending) >= numSlots {
			break
		}
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != groupFrom {
			continue
		}
		if m.GroupId == g.Id {
			continue
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *DashCore) SlotCreateActionInit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	groupIds := ctx.getGroupIds()
	if len(groupIds) >= MaxSlotNum {
		return errors.Errorf("too many groups.%+v", groupIds)
	}
	if mapping := ctx.getSlotMappingGroupIds(); !(len(mapping) == 1 && mapping[0] == 0) {
		return errors.New("could not reinit slot.")
	}
	remainder := MaxSlotNum % len(groupIds)
	quotient := MaxSlotNum / len(groupIds)
	beg, end := 0, 0
	must := true

	for gid, _ := range groupIds {
		end = beg + quotient - 1
		if remainder > 0 {
			end++
			remainder--
		}
		g, err := ctx.getGroup(gid)
		if err != nil {
			return err
		}
		if len(g.Servers) == 0 {
			return errors.Errorf("group-[%d] is empty", g.Id)
		}

		var pending []int
		for sid := beg; sid <= end; sid++ {
			m, err := ctx.getSlotMapping(sid)
			if err != nil {
				return err
			}
			if m.Action.State != models.ActionNothing {
				if !must {
					continue
				}
				return errors.Errorf("slot-[%d] action already exists", sid)
			}
			if m.GroupId == g.Id {
				if !must {
					continue
				}
				return errors.Errorf("slot-[%d] already in group-[%d]", sid, g.Id)
			}
			pending = append(pending, m.Id)
		}

		for _, sid := range pending {
			m, err := ctx.getSlotMapping(sid)
			if err != nil {
				return err
			}
			defer s.dirtySlotsCache(m.Id)

			m.Action.NotMigrateData = true
			m.Action.State = models.ActionPending
			m.Action.Index = ctx.maxSlotActionIndex() + 1
			m.Action.TargetId = g.Id
			if err := s.storeUpdateSlotMapping(m); err != nil {
				return err
			}
		}
		beg = end + 1
	}
	return nil
}

func (s *DashCore) SlotCreateActionRange(beg, end int, gid int, must, notMigrateData bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if !(beg >= 0 && beg <= end && end < MaxSlotNum) {
		return errors.Errorf("invalid slot range [%d,%d]", beg, end)
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for sid := beg; sid <= end; sid++ {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		if m.Action.State != models.ActionNothing {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] action already exists", sid)
		}
		if m.GroupId == g.Id {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] already in group-[%d]", sid, g.Id)
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.NotMigrateData = notMigrateData
		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *DashCore) SlotRemoveAction(sid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State == models.ActionNothing {
		return errors.Errorf("slot-[%d] action doesn't exist", sid)
	}
	if m.Action.State != models.ActionPending {
		return errors.Errorf("slot-[%d] action isn't pending", sid)
	}
	defer s.dirtySlotsCache(m.Id)

	m = &models.SlotMapping{
		Id:      m.Id,
		GroupId: m.GroupId,
	}
	return s.storeUpdateSlotMapping(m)
}

func (s *DashCore) SlotActionPrepare() (int, bool, error) {
	return s.SlotActionPrepareFilter(nil, nil)
}

func (s *DashCore) SlotActionMigratingMinIndex() (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return 0, false, err
	}

	for _, m := range ctx.slots {
		if m.Action.State == models.ActionMigrating {
			return m.Id, true, nil
		}
	}
	return 0, false, nil
}

func (s *DashCore) SlotActionPrepareFilter(accept, update func(m *models.SlotMapping) bool) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return 0, false, err
	}

	var minActionIndex = func(filter func(m *models.SlotMapping) bool) (picked *models.SlotMapping) {
		for _, m := range ctx.slots {
			if m.Action.State == models.ActionNothing {
				continue
			}
			if filter(m) {
				if picked != nil && picked.Action.Index < m.Action.Index {
					continue
				}
				if accept == nil || accept(m) {
					picked = m
				}
			}
		}

		return picked
	}

	var m = func() *models.SlotMapping {
		var picked = minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State != models.ActionPending
		})
		if picked != nil {
			return picked
		}
		if s.action.disabled.IsTrue() {
			return nil
		}
		return minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State == models.ActionPending
		})
	}()

	if m == nil {
		return 0, false, nil
	}

	if update != nil && !update(m) {
		return 0, false, nil
	}

	log.Infof("slot-[%d] action prepare: %s", m.Id, m.Encode())

	switch m.Action.State {
	case models.ActionPending:
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPreparing
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}
		fallthrough
	case models.ActionPreparing:
		defer s.dirtySlotsCache(m.Id)
		log.Warnf("slot-[%d] resync to preparing", m.Id)

		m.Action.State = models.ActionPrepared

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}
		fallthrough
	case models.ActionPrepared:
		defer s.dirtySlotsCache(m.Id)
		log.Warnf("slot-[%d] resync to prepared", m.Id)

		m.Action.State = models.ActionMigrating

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}
		fallthrough
	case models.ActionMigrating:
		return m.Id, true, nil
	case models.ActionFinished:
		return m.Id, true, nil
	default:
		return 0, false, errors.Errorf("slot-[%d] action state is invalid", m.Id)
	}
}

func (s *DashCore) SlotActionComplete(sourceAddr string, sid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}

	log.Warnf("slot-[%d] action complete : %s", m.Id, m.Encode())

	switch m.Action.State {

	case models.ActionMigrating:
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionFinished
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}

		fallthrough
	case models.ActionFinished:
		log.Warnf("slot-[%d] resync to finished", m.Id)

		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync to finished failed", m.Id)
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		if s.adminModel == RaftAdminModel && len(sourceAddr) > 0 {
			c, err := s.action.redisp.GetClient(sourceAddr)
			if err != nil {
				return err
			}
			defer s.action.redisp.PutClient(c)
			c.MigrateEnd(sid)
		}

		m = &models.SlotMapping{
			Id:      m.Id,
			GroupId: m.Action.TargetId,
		}
		return s.storeUpdateSlotMapping(m)
	default:
		return errors.Errorf("slot-[%d] action state is invalid", m.Id)
	}
}

func (s *DashCore) newSlotActionExecutor(sid int) (func() (needMigrate bool, sourceAddr string, sourceGroupID, targetGroupID int, err error), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return nil, err
	}

	switch m.Action.State {
	case models.ActionMigrating:
		if s.action.disabled.IsTrue() {
			return nil, nil
		}
		if m.GroupId == 0 {
			return nil, ErrInitGroupID
		}
		if ctx.isGroupPromoting(m.GroupId) {
			return nil, nil
		}
		if ctx.isGroupPromoting(m.Action.TargetId) {
			return nil, nil
		}

		from := ctx.getGroupMaster(m.GroupId)
		dest := ctx.getGroupMaster(m.Action.TargetId)

		s.action.executor.Incr()

		return func() (bool, string, int, int, error) {
			defer s.action.executor.Decr()
			if from == "" {
				return false, from, m.GroupId, m.Action.TargetId, nil
			}

			if m.Action.NotMigrateData {
				return false, from, m.GroupId, m.Action.TargetId, nil
			}
			c, err := s.action.redisp.GetClient(from)
			if err != nil {
				return false, from, m.GroupId, m.Action.TargetId, err
			}
			defer s.action.redisp.PutClient(c)

			if err := c.MigrateSlots(sid, dest); err != nil {
				return false, from, m.GroupId, m.Action.TargetId, err
			} else {
				return true, from, m.GroupId, m.Action.TargetId, nil
			}
		}, nil

	case models.ActionFinished:
		return func() (bool, string, int, int, error) {
			from := ctx.getGroupMaster(m.GroupId)
			return !m.Action.NotMigrateData, from, m.GroupId, m.Action.TargetId, nil
		}, nil

	default:
		return nil, errors.Errorf("slot-[%d] action state is invalid", m.Id)
	}
}

func (s *DashCore) getSlotActionMigrateStatus(sourceAddr string, sid int) (*models.MigrateStatus, error) {
	c, err := s.action.redisp.GetClient(sourceAddr)
	if err != nil {
		return nil, err
	}
	defer s.action.redisp.PutClient(c)

	if migrateStatusByte, err := c.MigrateStatus(sid); err != nil {
		return nil, err
	} else {
		migrateStatus := new(models.MigrateStatus)
		if err := json.Unmarshal(migrateStatusByte, migrateStatus); err == nil {
			if migrateStatus.Total != 0 {
				migrateStatus.SuccPercent = fmt.Sprintf("%0.6f", float64(migrateStatus.Total-migrateStatus.Fails)/float64(migrateStatus.Total))
			}
			return migrateStatus, nil
		} else {
			return nil, err
		}
	}
}

func (s *DashCore) SlotsAssignGroup(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		g, err := ctx.getGroup(m.GroupId)
		if err != nil {
			return err
		}
		if len(g.Servers) == 0 {
			return errors.Errorf("group-[%d] is empty", g.Id)
		}
		if m.Action.State != models.ActionNothing {
			return errors.Errorf("invalid slot-[%d] action = %s", m.Id, m.Action.State)
		}
	}

	for i, m := range slots {
		if g := ctx.group[m.GroupId]; !g.OutOfSync {
			defer s.dirtyGroupCache(g.Id)
			g.OutOfSync = true
			if err := s.storeUpdateGroup(g); err != nil {
				return err
			}
		}
		slots[i] = &models.SlotMapping{
			Id: m.Id, GroupId: m.GroupId,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d]", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *DashCore) SlotsAssignOffline(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		if m.GroupId != 0 {
			return errors.Errorf("group of slot-[%d] should be 0", m.Id)
		}
	}

	for i, m := range slots {
		slots[i] = &models.SlotMapping{
			Id: m.Id,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d] (offline)", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *DashCore) SlotsRebalance(confirm bool) (map[int]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	var groupIds []int
	for _, g := range ctx.group {
		if len(g.Servers) != 0 {
			groupIds = append(groupIds, g.Id)
		}
	}
	sort.Ints(groupIds)

	if len(groupIds) == 0 {
		return nil, errors.Errorf("no valid group could be found")
	}

	var (
		assigned = make(map[int]int)
		pendings = make(map[int][]int)
		moveout  = make(map[int]int)
		docking  []int
	)
	var groupSize = func(gid int) int {
		return assigned[gid] + len(pendings[gid]) - moveout[gid]
	}

	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			assigned[m.Action.TargetId]++
		}
	}

	var lowerBound = MaxSlotNum / len(groupIds)

	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			if groupSize(m.GroupId) < lowerBound {
				assigned[m.GroupId]++
			} else {
				pendings[m.GroupId] = append(pendings[m.GroupId], m.Id)
			}
		}
	}

	var tree = rbtree.NewWith(func(x, y interface{}) int {
		var gid1 = x.(int)
		var gid2 = y.(int)
		if gid1 != gid2 {
			if d := groupSize(gid1) - groupSize(gid2); d != 0 {
				return d
			}
			return gid1 - gid2
		}
		return 0
	})
	for _, gid := range groupIds {
		tree.Put(gid, nil)
	}

	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		docking = append(docking, m.Id)
		moveout[dest]--

		tree.Put(dest, nil)
	}

	var upperBound = (MaxSlotNum + len(groupIds) - 1) / len(groupIds)

	// rebalance between different server groups
	for tree.Size() >= 2 {
		from := tree.Right().Key.(int)
		tree.Remove(from)

		if len(pendings[from]) == moveout[from] {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		var (
			fromSize = groupSize(from)
			destSize = groupSize(dest)
		)
		if fromSize <= lowerBound {
			break
		}
		if destSize >= upperBound {
			break
		}
		if d := fromSize - destSize; d <= 1 {
			break
		}
		moveout[from]++
		moveout[dest]--

		tree.Put(from, nil)
		tree.Put(dest, nil)
	}

	for gid, n := range moveout {
		if n < 0 {
			continue
		}
		if n > 0 {
			sids := pendings[gid]
			sort.Sort(sort.Reverse(sort.IntSlice(sids)))

			docking = append(docking, sids[0:n]...)
			pendings[gid] = sids[n:]
		}
		delete(moveout, gid)
	}
	sort.Ints(docking)

	var plans = make(map[int]int)

	for _, gid := range groupIds {
		var in = -moveout[gid]
		for i := 0; i < in && len(docking) != 0; i++ {
			plans[docking[0]] = gid
			docking = docking[1:]
		}
	}

	if !confirm {
		return plans, nil
	}

	var slotIds []int
	for sid, _ := range plans {
		slotIds = append(slotIds, sid)
	}
	sort.Ints(slotIds)

	for _, sid := range slotIds {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return nil, err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = plans[sid]
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return nil, err
		}
	}
	return plans, nil
}
