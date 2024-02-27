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

package btools

type FlushType int

const (
	FlushTypeDbFlush    FlushType = 1
	FlushTypeCheckpoint FlushType = 2
	FlushTypeRemoveLog  FlushType = 3
	FlushTypeDbClose    FlushType = 4
)

func (f FlushType) String() string {
	switch f {
	case FlushTypeDbFlush:
		return "dbFlush"
	case FlushTypeCheckpoint:
		return "checkpoint"
	case FlushTypeRemoveLog:
		return "removeLog"
	case FlushTypeDbClose:
		return "dbClose"
	default:
		return "unknown"
	}
}
