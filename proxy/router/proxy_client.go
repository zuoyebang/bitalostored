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
	"sync"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/resp"
)

var doOnce = sync.Once{}
var globalProxyClient *ProxyClient = nil

type ProxyClient struct {
	mu       sync.Mutex
	readOnly bool
	router   *Router
}

func NewProxyClient(cfg *config.Config) *ProxyClient {
	doOnce.Do(func() {
		globalProxyClient = &ProxyClient{
			router:   NewRouter(cfg),
			readOnly: cfg.ReadOnlyProxy,
		}
	})
	return globalProxyClient
}

func GetProxyClient() (*ProxyClient, error) {
	if globalProxyClient == nil {
		return nil, errn.ErrNotInitProxy
	}
	return globalProxyClient, nil
}

func (pc *ProxyClient) GetAllGroup() map[int]int {
	groupMap := make(map[int]int, 1)
	for _, slot := range pc.router.slots {
		if _, ok := groupMap[slot.MasterAddrGroupId]; !ok {
			groupMap[slot.MasterAddrGroupId] = slot.Id
		}
	}
	return groupMap
}

func (pc *ProxyClient) GetMasterClients() (map[int]*resp.InternalServerConn, error) {
	groups := pc.GetAllGroup()
	clients := make(map[int]*resp.InternalServerConn, len(groups))
	var pool *InternalPool
	var err error
	for gid, s := range groups {
		if pool, err = pc.router.GetMasterConn(s); err != nil {
			return nil, err
		}

		clients[gid] = &resp.InternalServerConn{
			GroupId:  gid,
			Conn:     pool.GetConn(),
			HostPort: pool.GetHostPort(),
		}
	}
	return clients, nil
}

func (pc *ProxyClient) Slots() []*models.Slot {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.router.GetSlots()
}

func (pc *ProxyClient) FillSlot(slot *models.Slot) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if err := pc.router.FillSlot(slot); err != nil {
		return err
	}

	return nil
}

func (pc *ProxyClient) FillSlots(slots []*models.Slot) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for _, m := range slots {
		if err := pc.router.FillSlot(m); err != nil {
			log.Infof("fillslot failed slotId:%d err:%s", m.Id, err.Error())
			return err
		}
	}
	return nil
}

func (pc *ProxyClient) DoProbeNode() {
	pc.router.probe.doCheck()
}

func (pc *ProxyClient) FillDks(dks []*models.DkItem) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for _, d := range dks {
		if len(d.Key) <= 0 {
			continue
		}
		_, shardNum, _, _, err := pc.GetDkInfo(d.Key)
		if err != nil {
			return err
		}
		if shardNum > 0 {
			_ = pc.SetSimpleCacheWithExpire(d.Key, shardNum, DkExpireTime)
		} else {
			pc.RemoveSimpleCache(d.Key)
		}
	}
	return nil
}

func (pc *ProxyClient) CreateDk(dk *models.DkItem) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if dk.ShardNum <= 0 || len(dk.Key) <= 0 {
		return errors.New("shardnum or key error")
	}
	if !dk.CheckDt() {
		return errors.New("dt error")
	}
	sn, _ := pc.GetSimpleCache(dk.Key)
	if sn != nil {
		return nil
	}
	return pc.CreateDkToServer(unsafe2.ByteSlice(dk.Key), dk.ShardNum, dk.DataType)
}

func (pc *ProxyClient) CreateDkToServer(key []byte, shardNum uint32, dataType string) error {
	var j uint32
	groupKeys := make([]interface{}, 0, shardNum)
	for j = 0; j < shardNum; j++ {
		k := EncodeDkGroupKey(key, j)
		groupKeys = append(groupKeys, k)
	}
	res, _ := pc.DkCreateShard(dataType, groupKeys...)
	r := res.([]interface{})
	for _, rt := range r {
		switch rt.(type) {
		case error:
			pc.Del(nil, groupKeys...)
			return rt.(error)
		}
	}
	_, err := pc.DkCreate(key, shardNum, dataType)
	if err != nil {
		pc.Del(nil, groupKeys...)
		return err
	}
	return nil
}

func (pc *ProxyClient) RemoveDk(key string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if len(key) <= 0 {
		return errors.New("key error")
	}

	return pc.RemoveDkToServer(key)
}

func (pc *ProxyClient) RemoveDkToServer(key string) error {
	cacheSn, err := pc.GetDkShardNum(key)
	if err != nil {
		return err
	}
	if cacheSn <= 0 {
		return nil
	}
	var j uint32
	byteKey := unsafe2.ByteSlice(key)
	groupKeys := make([]interface{}, 0, cacheSn)
	for j = 0; j < cacheSn; j++ {
		k := EncodeDkGroupKey(byteKey, j)
		groupKeys = append(groupKeys, k)
	}
	res, _ := pc.Del(nil, groupKeys...)
	n := res.(int64)
	if n <= 0 {
		return nil
	}
	pc.RemoveSimpleCache(key)
	res, _ = pc.Del(nil, key)
	return nil
}
