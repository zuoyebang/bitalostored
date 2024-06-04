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

package proxy

import (
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/router"
)

func FillPconfigs(pconfigs []*models.Pconfig) error {
	proxyClient, _ := router.GetProxyClient()
	return proxyClient.FillPconfigs(pconfigs)
}

func Pconfigs() []*models.Pconfig {
	proxyClient, _ := router.GetProxyClient()
	return proxyClient.Pconfigs()
}

func CheckIsBlackKey(key string) bool {
	proxyClient, _ := router.GetProxyClient()
	return proxyClient.CheckIsBlackKey(key)
}

func CheckIsWhiteKey(key string) bool {
	proxyClient, _ := router.GetProxyClient()
	return proxyClient.CheckIsWhiteKey(key)
}

func FillSlots(slots []*models.Slot) error {
	proxyClient, _ := router.GetProxyClient()
	return proxyClient.FillSlots(slots)
}

func ShortSlots() []*models.Slot {
	proxyClient, _ := router.GetProxyClient()
	slots := proxyClient.Slots()

	shortSlots := make([]*models.Slot, 0, 10)
	groupMap := make(map[int]struct{}, 10)
	for _, slot := range slots {
		gid := slot.MasterAddrGroupId
		if _, ok := groupMap[gid]; ok {
			continue
		}
		groupMap[gid] = struct{}{}
		shortSlots = append(shortSlots, slot)
	}
	return shortSlots
}
