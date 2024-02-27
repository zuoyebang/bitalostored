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

package anticc

import (
	"errors"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/dostats"

	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/zuoyebang/bitalostored/butils/timesize"
)

var Enable bool

func Init() {
	if len(config.GlobalConfig.DynamicDeadline.ClientRatios) == 0 || len(config.GlobalConfig.DynamicDeadline.DeadlineThreshold) == 0 {
		return
	}
	err := LoadConfig(config.GlobalConfig.DynamicDeadline.ClientRatios, config.GlobalConfig.DynamicDeadline.DeadlineThreshold, int(config.GlobalConfig.Server.Maxclient))
	if err != nil {
		log.Errorf("load anticc config fail err:%s", err.Error())
		return
	}
	Enable = true
}

func LoadConfig(aliveConnRatios []int, deadlineThreshold []timesize.Duration, maxClients int) error {
	if len(aliveConnRatios) != len(deadlineThreshold) {
		return errors.New("length of array client_ratio_threshold and deadline_threshold should be equal")
	}
	if len(aliveConnRatios) == 0 {
		return errors.New("missing client deadline config")
	}
	dd.aliveConnRatios = aliveConnRatios[:]
	dd.deadlineThreshold = deadlineThreshold[:]
	dd.maxClients = maxClients
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
				return time.Now().Add(dd.deadlineThreshold[0].Duration())
			}
			return time.Now().Add(dd.deadlineThreshold[index-1].Duration())
		}
	}
	t := dd.deadlineThreshold[len(dd.deadlineThreshold)-1].Duration()
	return time.Now().Add(t)
}
