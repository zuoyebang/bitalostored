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

package router

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/math2"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/internal/gcache"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/internal/switcher"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/panjf2000/ants/v2"
	"github.com/sony/gobreaker"
)

const (
	MaxSlotNum = models.MaxSlotNum

	AntsPoolSize = 96
)

const (
	CloudTypeLocal  = "local"
	CloudTypeBackup = "backup"
)

const (
	MgetCommandType = iota + 1
	MsetCommandType
	DelCommandType
	WatchCommandType
	UnwatchCommandType
	MultiCommandType
	PrepareCommandType
	ExecCommandType
	DiscardCommandType
)

type Router struct {
	mu            sync.RWMutex
	slots         []*models.Slot
	groupPools    sync.Map
	antsPools     sync.Map
	GroupBreaker  *GroupBreaker
	localCache    *gcache.BucketCache
	config        *config.Config
	online        bool
	closed        bool
	curPoolActive int
	probe         *probeTask
}

func NewRouter(config *config.Config) *Router {
	r := &Router{
		config:        config,
		slots:         make([]*models.Slot, MaxSlotNum),
		groupPools:    sync.Map{},
		antsPools:     sync.Map{},
		localCache:    gcache.NewBucketCache(DefaultLocalCacheExpireTime, 4*time.Minute, 8),
		online:        true,
		closed:        false,
		curPoolActive: config.RedisDefaultConf.MaxActive,
	}
	dostats.SetPoolActive(r.curPoolActive)
	for i := range r.slots {
		r.slots[i] = &models.Slot{Id: i}
	}
	r.probe = newProbeTask(r)
	r.GroupBreaker = NewGroupBreaker(config)
	r.FlushGlobalStat()
	return r
}

func (r *Router) Close() {
	r.closed = true
	r.online = false
}

func (r *Router) GetConn(
	slotId int, command string,
) (interPool *InternalPool, needCircuit bool, curindex uint64, cloudType string, err error) {
	slot := r.GetSlot(slotId)

	if IsWriteCmd(command) || checkSlotLocalEmptyAndBackupEmpty(slot) {
		if slot.MasterAddr == "" {
			return nil, false, 0, "", fmt.Errorf("slot-%d master addr is empty", slot.Id)
		}
		if ipool, ok := r.GetAddrPool(slot.MasterAddr); ok {
			return ipool, false, 0, "", nil
		}

		return nil, false, 0, "", fmt.Errorf("slot-%d master pool is empty", slot.Id)
	}

	localNum := len(slot.LocalCloudServers)
	if localNum > 0 {
		if localNum == 1 {
			if ipool, ok := r.GetAddrPool(slot.LocalCloudServers[0]); ok {
				return ipool, true, 0, CloudTypeLocal, nil
			}
		} else {
			index := slot.RoundRobinNum % uint64(localNum)
			if slot.LocalCloudServers[index] == slot.MasterAddr {
				if !math2.ChanceControl(r.config.ReadMasterChance) {
					indexIncr := atomic.AddUint64(&slot.RoundRobinNum, math2.ChanceDelta(localNum-1))
					index = indexIncr % uint64(localNum)
				}
			}
			if ipool, ok := r.GetAddrPool(slot.LocalCloudServers[index]); ok {
				return ipool, true, index, CloudTypeLocal, nil
			}
		}
	}

	backupNum := len(slot.BackupCloudServers)
	if backupNum > 0 && switcher.ReadCrossCloud.Load() {
		if backupNum == 1 {
			if ipool, ok := r.GetAddrPool(slot.BackupCloudServers[0]); ok {
				return ipool, false, 0, CloudTypeBackup, nil
			}
		} else {
			indexIncr := atomic.AddUint64(&slot.RoundRobinNum, 1)
			index := indexIncr % uint64(backupNum)
			if slot.BackupCloudServers[index] == slot.MasterAddr {
				if !math2.ChanceControl(r.config.ReadMasterChance) {
					indexIncr = atomic.AddUint64(&slot.RoundRobinNum, math2.ChanceDelta(backupNum-1))
					index = indexIncr % uint64(backupNum)
				}
			}
			if ipool, ok := r.GetAddrPool(slot.BackupCloudServers[index]); ok {
				return ipool, true, index, CloudTypeBackup, nil
			}
		}

		return nil, false, 0, "", fmt.Errorf("slot-%d backup cloud servers index is empty", slot.Id)
	}

	return nil, false, 0, "", fmt.Errorf("slot-%d no server resource", slot.Id)
}

func (r *Router) GetMasterConn(slotId int) (interPool *InternalPool, err error) {
	slot := r.GetSlot(slotId)
	if slot.MasterAddr == "" {
		return nil, fmt.Errorf("slot-%d master addr is empty", slot.Id)
	}
	if ipool, ok := r.GetAddrPool(slot.MasterAddr); ok {
		return ipool, nil
	}
	return nil, fmt.Errorf("slot-%d master pool is empty", slot.Id)
}

func (r *Router) GetAnotherConnByCircuit(slotId int, command string, prevIndex uint64, prevCloud string) (*InternalPool, uint64, error) {
	slot := r.GetSlot(slotId)
	noServerError := fmt.Errorf("slot-%d no server resource", slot.Id)

	if IsWriteCmd(command) {
		return nil, 0, fmt.Errorf("slot-%d write not support circuit", slot.Id)
	}

	var hystrixName string
	var cgb *Breaker
	if groupId, err := r.GetGroupId(slotId); err == nil {
		if cgb, err = r.GroupBreaker.GetCircuitBreakerByGid(groupId); err != nil {
			cgb = nil
		}
	}

	chooseAvailableServer := func(servers []string, serverNum int, cloudType string) (*InternalPool, uint64, error) {
		index := slot.RoundRobinNum % uint64(serverNum)
		for i := 0; i < serverNum; i++ {
			if index == prevIndex && cloudType == prevCloud {
				index = (index + 1) % uint64(serverNum)
				continue
			}

			if ipool, ok := r.GetAddrPool(servers[index]); ok {
				if cgb == nil {
					return ipool, index, nil
				}
				hystrixName = ipool.GetHostPort()
				cb := cgb.GetCircuitBreaker(hystrixName)
				if cb.State() != gobreaker.StateOpen {
					return ipool, index, nil
				}
			}
			index = (index + 1) % uint64(serverNum)
		}
		return nil, 0, noServerError
	}

	localNum := len(slot.LocalCloudServers)
	if localNum > 1 {
		ipool, index, err := chooseAvailableServer(slot.LocalCloudServers, localNum, CloudTypeLocal)
		if err == nil {
			return ipool, index, nil
		}
	}

	backupNum := len(slot.BackupCloudServers)
	if backupNum > 0 && switcher.ReadCrossCloud.Load() {
		if backupNum == 1 {
			if ipool, ok := r.GetAddrPool(slot.BackupCloudServers[0]); ok {
				return ipool, 0, nil
			}
			return nil, 0, noServerError
		} else {
			return chooseAvailableServer(slot.BackupCloudServers, backupNum, CloudTypeBackup)
		}
	}

	return nil, 0, noServerError
}

func (r *Router) GetSlots() []*models.Slot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	slots := make([]*models.Slot, MaxSlotNum)
	for i := range r.slots {
		slots[i] = r.slots[i].Snapshot(false)
	}
	return slots
}

func (r *Router) PoolStats() InternalPoolStat {
	slots := r.GetSlots()

	var poolStats InternalPoolStat
	groupMap := make(map[int]struct{}, 10)
	for _, s := range slots {
		if _, ok := groupMap[s.MasterAddrGroupId]; ok {
			continue
		} else {
			groupMap[s.MasterAddrGroupId] = struct{}{}
			servers := make([]string, 0, len(s.LocalCloudServers)+len(s.BackupCloudServers))
			servers = append(servers, s.LocalCloudServers...)
			servers = append(servers, s.BackupCloudServers...)
			for _, addr := range servers {
				if pool, exist := r.GetAddrPool(addr); exist {
					s := pool.Stats()
					poolStats.ActiveCount += s.ActiveCount
					poolStats.IdleCount += s.IdleCount
				}
			}
		}
	}
	return poolStats
}

func (r *Router) FlushGlobalStat() {
	go func() {
		for !r.closed {
			s := r.PoolStats()
			dostats.SetPoolStat(s.ActiveCount, s.IdleCount)
			time.Sleep(20 * time.Second)
		}
	}()
}

func (r *Router) GetAddrPool(addr string) (*InternalPool, bool) {
	if res, ok := r.groupPools.Load(addr); ok {
		return res.(*InternalPool), ok
	}

	return nil, false
}

func (r *Router) GetAntsPool(addr string) (*ants.PoolWithFunc, bool) {
	if res, ok := r.antsPools.Load(addr); ok {
		return res.(*ants.PoolWithFunc), ok
	}

	return nil, false
}

func (r *Router) GetSlot(id int) *models.Slot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id < 0 || id >= MaxSlotNum {
		return nil
	}
	slot := r.slots[id].Snapshot(true)
	return slot
}

func (r *Router) FillSlot(m *models.Slot) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	if m.Id < 0 || m.Id >= MaxSlotNum {
		return errn.ErrInvalidSlotId
	}

	addrs := make([]string, 0, len(m.GroupServersCloudMap)+len(m.WitnessServers))
	for server := range m.GroupServersCloudMap {
		addrs = append(addrs, server)
	}
	if len(m.WitnessServers) > 0 {
		addrs = append(addrs, m.WitnessServers...)
	}
	r.newGroupPool(m.MasterAddrGroupId, addrs, r.curPoolActive)
	r.slots[m.Id] = m

	return nil
}

func (r *Router) registerGroupBreaker(groupId int, addr string) {
	r.GroupBreaker.AddCircuitBreaker(groupId, addr)
}

func (r *Router) delGroupBreakerByAddrs(groupId int, addr string) {
	r.GroupBreaker.RemoveCircuitBreaker(groupId, addr)
}

func (r *Router) newGroupPool(groupId int, addrs []string, poolMaxActive int) {
	if len(addrs) <= 0 {
		return
	}

	for _, addr := range addrs {
		if _, ok := r.groupPools.Load(addr); !ok {
			r.registerGroupBreaker(groupId, addr)

			poolConf := r.config.RedisDefaultConf
			poolConf.HostPort = addr
			poolConf.MaxActive = poolMaxActive
			poolConf.MaxIdle = poolMaxActive
			pool := &InternalPool{
				HostPort: addr,
				Pool:     GetPool(poolConf),
			}
			r.groupPools.Store(addr, pool)

			antsPool, _ := ants.NewPoolWithFunc(
				AntsPoolSize,
				multiCommandAntsCallback,
				ants.WithExpiryDuration(60*time.Second),
				ants.WithPreAlloc(true))
			r.antsPools.Store(addr, antsPool)

			log.Infof("newGroupPool add addr:%s", addr)
		}
	}
}

func (r *Router) GetGroupId(slotId int) (int, error) {
	if slotId < 0 || slotId >= len(r.slots) {
		return 0, errors.New("get group id out of range ")
	}
	if slot := r.slots[slotId]; slot == nil {
		return 0, errors.New("get slot is nil")
	} else {
		if slot.MasterAddrGroupId <= 0 {
			return 0, errors.New("slot group id is small than 0")
		}
		return slot.MasterAddrGroupId, nil
	}
}

func (r *Router) Hash(key interface{}) int {
	switch key.(type) {
	case string:
		keyByte := unsafe2.ByteSlice(key.(string))
		return int(hash.Fnv32(keyByte) % MaxSlotNum)
	case []byte:
		keyByte := key.([]byte)
		return int(hash.Fnv32(keyByte) % MaxSlotNum)
	default:
		return -1
	}
}

func (r *Router) HashForLua(key string) int {
	keybyte := resp.ExtractHashTag(key)
	index := hash.Fnv32(keybyte) % MaxSlotNum
	return int(index)
}

func checkSlotLocalEmptyAndBackupEmpty(slot *models.Slot) bool {
	if len(slot.LocalCloudServers) <= 0 && len(slot.BackupCloudServers) <= 0 {
		return true
	}
	return false
}
