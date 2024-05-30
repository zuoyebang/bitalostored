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

package anticc

import (
	"errors"
	"time"

	"github.com/zuoyebang/bitalostored/butils/timesize"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
)

func LoadConfig(cfg *config.Config) error {
	if len(cfg.DynamicDeadline.ClientRatios) == 0 {
		return errors.New("missing client deadline config")
	}

	if len(cfg.DynamicDeadline.ClientRatios) != len(cfg.DynamicDeadline.DeadlineThreshold) {
		return errors.New("length of array client_ratio_threshold and deadline_threshold should be equal")
	}

	dd.aliveConnRatios = cfg.DynamicDeadline.ClientRatios[:]
	dd.deadlineThreshold = cfg.DynamicDeadline.DeadlineThreshold[:]
	dd.maxClients = cfg.ProxyMaxClients
	return nil
}

type dynamicDeadline struct {
	maxClients        int
	aliveConnRatios   []int
	deadlineThreshold []timesize.Duration
}

var dd dynamicDeadline

func GetConfigDeadline() time.Time {
	for index, connThreshold := range dd.aliveConnRatios {
		if (dostats.ConnsAlive()*100)/int64(dd.maxClients) < int64(connThreshold) {
			if index == 0 {
				log.Errorf("wrong config.dynamicDeadline:%+v.alive conn:%d", dd, dostats.ConnsAlive())
				return time.Now().Add(dd.deadlineThreshold[0].Duration())
			}
			return time.Now().Add(dd.deadlineThreshold[index-1].Duration())
		}
	}
	t := dd.deadlineThreshold[len(dd.deadlineThreshold)-1].Duration()
	return time.Now().Add(t)
}
