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

import (
	"math"

	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/butils/numeric"
)

const (
	DefaultScanCount   int    = 10
	LuaScriptSlot      uint16 = 2048
	KeyLockerPoolCap   uint32 = 16 << 10
	ConfigMaxFieldSize int    = 60 << 10
	ConfigMaxValueSize int    = 8 << 20
)

var (
	MaxKeySize               = 512
	MaxFieldSize             = 10 << 10
	MaxValueSize             = 6 << 20
	MaxIOWriteLoadQPS uint64 = 20000
	MaxScoreByte             = numeric.Float64ToByteSort(math.MaxFloat64, nil)
	ScanEndCurosr            = []byte("0")
)

func SetDefineVarFromCfg() {
	if config.GlobalConfig.Bitalos.MaxFieldSize > 0 {
		if config.GlobalConfig.Bitalos.MaxFieldSize > ConfigMaxFieldSize {
			MaxFieldSize = ConfigMaxFieldSize
		} else {
			MaxFieldSize = config.GlobalConfig.Bitalos.MaxFieldSize
		}
	}

	if config.GlobalConfig.Bitalos.MaxValueSize > 0 {
		if config.GlobalConfig.Bitalos.MaxValueSize > ConfigMaxValueSize {
			MaxValueSize = ConfigMaxValueSize
		} else {
			MaxValueSize = config.GlobalConfig.Bitalos.MaxValueSize
		}
	}

	if config.GlobalConfig.Bitalos.IOWriteLoadQpsThreshold > 0 {
		MaxIOWriteLoadQPS = config.GlobalConfig.Bitalos.IOWriteLoadQpsThreshold
	}
}
