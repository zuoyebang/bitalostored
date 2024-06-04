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

package dostats

import (
	"sync/atomic"
)

var connStats struct {
	total atomic.Int64
	alive atomic.Int64
}

func IncrConns() int64 {
	connStats.total.Add(1)
	return connStats.alive.Add(1)
}

func DecrConns() {
	connStats.alive.Add(-1)
}

func ConnsTotal() int64 {
	return connStats.total.Load()
}

func ConnsAlive() int64 {
	return connStats.alive.Load()
}

var poolActive int

func SetPoolActive(n int) {
	poolActive = n
}

func GetPoolActive() int {
	return poolActive
}

type PoolStat struct {
	ActiveCount int `json:"active_count"`
	IdleCount   int `json:"idle_count"`
}

var poolStat PoolStat

func SetPoolStat(activeCount int, idleCount int) {
	poolStat.ActiveCount = activeCount
	poolStat.IdleCount = idleCount
}

func GetPoolStat() PoolStat {
	return poolStat
}
