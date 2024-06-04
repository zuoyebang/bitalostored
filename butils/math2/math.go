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

package math2

import (
	"fmt"
	"math/rand"
	"time"
)

func MaxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MinMaxInt(v, min, max int) int {
	if min <= max {
		v = MaxInt(v, min)
		v = MinInt(v, max)
		return v
	}
	panic(fmt.Sprintf("min = %d, max = %d", min, max))
}

func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	} else {
		return b
	}
}

func Abs(i, j int64) int64 {
	if i < j {
		return j - i
	}
	return i - j
}

func ChanceControl(hitPercentage int) bool {
	if hitPercentage <= 0 {
		return false
	}
	if hitPercentage >= 100 {
		return true
	}
	n := rand.Intn(100)
	if n >= hitPercentage {
		return false
	}
	return true
}

func ChanceDelta(ceiling int) uint64 {
	return uint64(rand.Intn(ceiling) + 1)
}
