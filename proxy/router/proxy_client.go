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

package router

import (
	"sync"

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
	pconfig  *PclientConfig
}

func NewProxyClient(cfg *config.Config) *ProxyClient {
	doOnce.Do(func() {
		globalProxyClient = &ProxyClient{
			router:   NewRouter(cfg),
			pconfig:  newPclientConfig(),
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

func (pc *ProxyClient) Pconfigs() []*models.Pconfig {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.pconfig.pconfigs()
}

func (pc *ProxyClient) FillSlot(slot *models.Slot) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if err := pc.router.FillSlot(slot); err != nil {
		return err
	}

	return nil
}

func (pc *ProxyClient) FillPconfigs(pconfigs []*models.Pconfig) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for _, m := range pconfigs {
		log.Infof("fill pconfig %s", string(m.Encode()))
		if err := pc.pconfig.fillPconfig(m); err != nil {
			return err
		}
	}
	return nil
}

func (pc *ProxyClient) CheckIsBlackKey(key string) bool {
	return pc.pconfig.checkIsBlackKey(key)
}

func (pc *ProxyClient) CheckIsWhiteKey(key string) bool {
	return pc.pconfig.checkIsWhiteKey(key)
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

func (pc *ProxyClient) checkKeyIsProxyCache(key string) bool {
	if pc.pconfig.checkIsBlackCache(key) {
		return false
	}
	if pc.pconfig.checkIsWhiteCache(key) {
		return true
	}
	return false
}

func (pc *ProxyClient) checkKeysSaveCache(keys ...interface{}) (bool, []string) {
	needCacheKey := make([]string, 0, len(keys))
	for _, key := range keys {
		if val, ok := key.(string); ok {
			if pc.checkKeyIsProxyCache(val) {
				needCacheKey = append(needCacheKey, val)
			}
		}
	}

	if len(needCacheKey) <= 0 {
		return false, nil
	}

	return true, needCacheKey
}
