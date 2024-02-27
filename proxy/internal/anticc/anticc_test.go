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
	"fmt"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/butils/timesize"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
)

func TestGetConfigDeadline(t *testing.T) {
	cfg := config.NewDefaultConfig()
	cfg.DynamicDeadline.ClientRatios = []int{0, 30, 60, 90}
	cfg.DynamicDeadline.DeadlineThreshold = []timesize.Duration{
		timesize.Duration(180 * time.Second),
		timesize.Duration(60 * time.Second),
		timesize.Duration(30 * time.Second),
		timesize.Duration(3 * time.Second)}
	cfg.ProxyMaxClients = 3000
	if err := LoadConfig(cfg); err != nil {
		t.Fail()
		return
	}
	increaseStatConn(200)
	if GetConfigDeadline().Unix() != time.Now().Add(180*time.Second).Unix() {
		t.Fail()
	}
	increaseStatConn(1000)
	if GetConfigDeadline().Unix() != time.Now().Add(60*time.Second).Unix() {
		fmt.Println(GetConfigDeadline(), " now:", time.Now())
		t.Fail()
	}
	increaseStatConn(1000)
	if GetConfigDeadline().Unix() != time.Now().Add(30*time.Second).Unix() {
		t.Fail()
	}
	increaseStatConn(700)
	if GetConfigDeadline().Unix() != time.Now().Add(3*time.Second).Unix() {
		t.Fail()
	}
	decreaseStatConn(1500)
	if GetConfigDeadline().Unix() != time.Now().Add(60*time.Second).Unix() {
		fmt.Println(GetConfigDeadline(), " now:", time.Now())
		t.Fail()
	}
	decreaseStatConn(800)
	if GetConfigDeadline().Unix() != time.Now().Add(180*time.Second).Unix() {
		fmt.Println(GetConfigDeadline(), " now:", time.Now())
		t.Fail()
	}
}

func increaseStatConn(delta int) {
	for i := 0; i < delta; i++ {
		dostats.IncrConns()
	}
}

func decreaseStatConn(delta int) {
	for i := 0; i < delta; i++ {
		dostats.DecrConns()
	}
}
