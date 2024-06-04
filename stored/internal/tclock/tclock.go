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

package tclock

import (
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
)

var IsStoreTime bool = false
var StoredTimeClock atomic.Int64

func InitTimeClock() {
	if config.GlobalConfig.Bitalos.EnableClockCache {
		IsStoreTime = true
		go func() {
			StoredTimeClock.Store(time.Now().UnixMilli())
			for {
				time.Sleep(time.Second)
				StoredTimeClock.Store(time.Now().UnixMilli())
			}
		}()
	}
}

func GetTimestampSecond() int64 {
	if IsStoreTime {
		return StoredTimeClock.Load() / 1e3
	} else {
		return time.Now().Unix()
	}
}

func GetTimestampMilli() int64 {
	if IsStoreTime {
		return StoredTimeClock.Load()
	} else {
		return time.Now().UnixMilli()
	}
}

func SetTtlMilliToSec(time int64) int64 {
	if time > 0 {
		return (time + 1e3 - 1) / 1e3
	}
	return time
}

func SetTimestampMilli(time int64) int64 {
	return time * 1e3
}

func SetExpireAtMilli(duration int64) int64 {
	sec := GetTimestampSecond()
	return SetTimestampMilli(sec + duration)
}

func GetYesterdayZeroTime() int64 {
	ts := time.Now().AddDate(0, 0, -1)
	timeYesterday := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location())
	timeStampYesterday := timeYesterday.UnixMilli()
	return timeStampYesterday
}
