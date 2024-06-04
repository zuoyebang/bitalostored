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

package info

import (
	"math"
	"runtime/debug"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const (
	RuntimeInterval = 4
	ClientInterval  = 16
	DiskInterval    = 120
)

func Init() {
	interval := time.Second

	server.AddPlugin(&server.Proc{Start: func(s *server.Server) {
		go func() {
			dataInterval := 60
			info := func() {
				defer func() {
					if r := recover(); r != nil {
						log.Errorf("plugin doinfo panic err:%v stack=%s", r, string(debug.Stack()))
					}
				}()

				start := time.Now()
				total := s.Info.Stats.TotolCmd.Load()

				time.Sleep(interval)

				delta := s.Info.Stats.TotolCmd.Load() - total
				normalized := math.Max(0, float64(delta)) * float64(time.Second) / float64(time.Since(start))
				qps := uint64(normalized + 0.5)
				s.Info.Stats.QPS.Store(qps)
				db := s.GetDB()
				if db != nil {
					db.SetQPS(qps)
					s.Info.Stats.RaftLogIndex = db.Meta.GetUpdateIndex()
					if db.Migrate != nil {
						s.Info.Stats.IsMigrate.Store(db.Migrate.IsMigrate.Load())
					}
					s.Info.Stats.IsDelExpire = db.GetIsDelExpire()
				}

				singleDegradeChange := s.Info.Server.SingleDegrade != config.GlobalConfig.Server.DegradeSingleNode
				s.Info.Server.SingleDegrade = config.GlobalConfig.Server.DegradeSingleNode
				if singleDegradeChange {
					s.Info.Server.UpdateCache()
				}

				if dataInterval%RuntimeInterval == 0 {
					s.Info.Stats.UpdateCache()
					s.Info.RuntimeStats.Samples()
				}

				if dataInterval%ClientInterval == 0 {
					s.Info.Client.UpdateCache()
				}

				if dataInterval%DiskInterval == 0 {
					s.Info.Data.Samples()
					if db != nil {
						db.BitalosdbUsage(s.Info.BitalosdbUsage)
					}
				}

				dataInterval++
			}

			for {
				info()
			}
		}()
	}})
}
