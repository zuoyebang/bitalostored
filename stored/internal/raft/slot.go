package raft

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const removedGroup = 0

type SlotGroupInfo struct {
	slot  uint16
	group uint64
}

type SlotMapping struct {
	mu        sync.RWMutex
	slotGroup map[uint16]uint64
	raftMeta  *RaftMeta
	spliting  atomic.Bool
	mr        *MultiRaft
}

func LoadSlotMapping(mr *MultiRaft, meta *RaftMeta, isMulti bool) *SlotMapping {
	ids := meta.GetAllSlots()
	s := &SlotMapping{
		slotGroup: make(map[uint16]uint64, btools.TotalSlot),
		raftMeta:  meta,
		mr:        mr,
	}
	var gid uint64
	var multiGroup bool
	onlineGroups := make(map[uint64]bool, 1)
	s.mu.Lock()
	for i := uint16(0); i < uint16(btools.TotalSlot); i++ {
		s.slotGroup[i] = ids[i]
		if !multiGroup && gid != 0 && gid != ids[i] {
			multiGroup = true
		}
		if ids[i] != removedGroup {
			gid = ids[i]
		}
		onlineGroups[gid] = true
	}
	s.mu.Unlock()
	mr.SetOnlineGroups(onlineGroups)
	if multiGroup && isMulti {
		s.spliting.Store(true)
	}
	return s
}

func NewSlotMapping(mr *MultiRaft, meta *RaftMeta, clusterId uint64) *SlotMapping {
	s := &SlotMapping{
		slotGroup: make(map[uint16]uint64, btools.TotalSlot),
		raftMeta:  meta,
		mr:        mr,
	}
	for i := uint16(0); i < uint16(btools.TotalSlot); i++ {
		s.slotGroup[i] = clusterId
	}
	return s
}

func (s *SlotMapping) IsSpliting() bool {
	return s.spliting.Load()
}

func (s *SlotMapping) updateGroups(slotStart, slotEnd uint16, group uint64) (oneGroup bool, groupId uint64, err error) {
	if !checkSlot(slotStart) || !checkSlot(slotEnd) {
		log.Errorf("slot is out of bound. s:%d e:%d", slotStart, slotEnd)
		return false, 0, errors.New("slot not allowed")
	}

	for i := slotStart; i <= slotEnd; i++ {
		s.mu.Lock()
		if s.slotGroup[i] == group {
			s.mu.Unlock()
			continue
		} else {
			s.slotGroup[i] = group
			s.mu.Unlock()
			s.raftMeta.SetSlotGroup(int(i), uint16(group))
		}
	}

	gid, onlyOneGroup := s.checkOneGroup()
	if onlyOneGroup {
		s.disableSplit(gid)
	} else {
		s.enableSplit()
	}
	return onlyOneGroup, gid, nil
}

func (s *SlotMapping) removeSlots(slotStart, slotEnd uint16) {
	for i := slotStart; i <= slotEnd; i++ {
		s.mu.Lock()
		if s.slotGroup[i] == removedGroup {
			s.mu.Unlock()
			continue
		}
		s.slotGroup[i] = removedGroup
		s.mu.Unlock()
		s.raftMeta.SetSlotGroup(int(i), removedGroup)
	}

	gid, onlyOneGroup := s.checkOneGroup()
	if onlyOneGroup {
		s.disableSplit(gid)
	}
}

func (s *SlotMapping) enableSplit() {
	s.mr.mu.Lock()
	s.spliting.Store(true)
	s.mr.refreshOnlineCluster(true)
	s.mr.mu.Unlock()
}

func (s *SlotMapping) disableSplit(gid uint64) {
	s.mr.mu.Lock()
	s.spliting.Store(false)
	s.mr.reserveOneGroupOnline(gid)
	s.mr.refreshOnlineCluster(true)
	s.mr.mu.Unlock()
}

func (s *SlotMapping) checkOneGroup() (uint64, bool) {
	var gid, currentGid uint64
	for i := uint16(0); i < uint16(btools.TotalSlot); i++ {
		s.mu.RLock()
		currentGid = s.slotGroup[i]
		s.mu.RUnlock()
		if currentGid == removedGroup {
			continue
		}
		// Init once
		if gid == 0 {
			gid = currentGid
			continue
		}
		if gid != currentGid {
			return 0, false
		}
	}
	return gid, true
}

func (s *SlotMapping) GetGroup(slot uint16) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if g, ok := s.slotGroup[slot]; ok {
		return g
	} else {
		return 0
	}
}

func checkSlot(slotId uint16) bool {
	return slotId >= 0 && slotId < uint16(btools.TotalSlot)
}
