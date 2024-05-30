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

type OpStats struct {
	Opstr       string
	Calls       atomic.Int64
	Nsecs       atomic.Int64
	Fails       atomic.Int64
	PeriodFails atomic.Int64
}

func (s *OpStats) OpStats() *CalOpStats {
	o := &CalOpStats{
		OpStr:       s.Opstr,
		Calls:       s.Calls.Load(),
		Usecs:       s.Nsecs.Load() / 1e3,
		Fails:       s.Fails.Load(),
		PeriodFails: s.PeriodFails.Load(),
	}
	if o.Calls != 0 {
		o.UsecsPercall = o.Usecs / o.Calls
	}
	return o
}

type CalOpStats struct {
	OpStr        string `json:"opstr"`
	Calls        int64  `json:"calls"`
	Usecs        int64  `json:"usecs"`
	UsecsPercall int64  `json:"usecs_percall"`
	Fails        int64  `json:"fails"`
	PeriodFails  int64  `json:"periodfails"`
}

type sliceCalOpStats []*CalOpStats

func (s sliceCalOpStats) Len() int {
	return len(s)
}

func (s sliceCalOpStats) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sliceCalOpStats) Less(i, j int) bool {
	return s[i].OpStr < s[j].OpStr
}
