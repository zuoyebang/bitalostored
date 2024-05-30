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

	"github.com/zuoyebang/bitalostored/proxy/internal/models"
)

type PclientConfig struct {
	mu     sync.RWMutex
	wblist []*models.WhiteAndBlackList
}

func newPclientConfig() *PclientConfig {
	return &PclientConfig{
		mu:     sync.RWMutex{},
		wblist: make([]*models.WhiteAndBlackList, 2),
	}
}

func (pcc *PclientConfig) pconfigs() []*models.Pconfig {
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	res := make([]*models.Pconfig, 0, 2)
	localCachePconfig := &models.Pconfig{
		Name:      models.LocalCachePrefix,
		Content:   pcc.wblist[models.LocalCacheIndex],
		OutOfSync: false,
	}
	blackPconfig := &models.Pconfig{
		Name:      models.BlackKeys,
		Content:   pcc.wblist[models.BlackKeysIndex],
		OutOfSync: false,
	}
	res = append(res, localCachePconfig, blackPconfig)
	return res
}

func (pcc *PclientConfig) fillPconfig(pconfig *models.Pconfig) error {
	pcc.mu.Lock()
	defer pcc.mu.Unlock()

	updateBlackWhiteList(pconfig)
	if pconfig.Name == models.LocalCachePrefix {
		pcc.wblist[models.LocalCacheIndex] = pconfig.Content
	} else if pconfig.Name == models.BlackKeys {
		pcc.wblist[models.BlackKeysIndex] = pconfig.Content
	} else {
		return errors.New("not exists pconfig name")
	}

	return nil
}

func updateBlackWhiteList(pconfig *models.Pconfig) {
	if len(pconfig.Content.Black) > 0 {
		pconfig.Content.BlackMap = make(map[string]bool)
		for _, name := range pconfig.Content.Black {
			pconfig.Content.BlackMap[name] = true
		}
	}
	if len(pconfig.Content.White) > 0 {
		pconfig.Content.WhiteMap = make(map[string]bool)
		for _, name := range pconfig.Content.White {
			pconfig.Content.WhiteMap[name] = true
		}
	}
	pconfig.BuildTrie()
}

func (pcc *PclientConfig) checkIsBlackKey(key string) bool {
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	if blackKeys := pcc.wblist[models.BlackKeysIndex]; blackKeys != nil {
		if len(blackKeys.Black) >= 0 && blackKeys.BlackMap[key] {
			return true
		}
	}
	return false
}

func (pcc *PclientConfig) checkIsWhiteKey(key string) bool {
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	if whiteKeys := pcc.wblist[models.BlackKeysIndex]; whiteKeys != nil {
		if len(whiteKeys.White) >= 0 && whiteKeys.WhiteMap[key] {
			return true
		}
	}
	return false
}

func (pcc *PclientConfig) checkIsBlackCache(key string) bool {
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	if blackCache := pcc.wblist[models.LocalCacheIndex]; blackCache != nil {
		if len(blackCache.Black) >= 0 && blackCache.BlackMap[key] {
			return true
		}
	}
	return false
}

func (pcc *PclientConfig) checkIsWhiteCache(key string) bool {
	pcc.mu.RLock()
	defer pcc.mu.RUnlock()
	if whiteCache := pcc.wblist[models.LocalCacheIndex]; whiteCache != nil {
		if len(whiteCache.White) >= 0 && whiteCache.WhiteMap[key] {
			return true
		}
	}
	return false
}
