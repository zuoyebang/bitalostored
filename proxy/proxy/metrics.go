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

package proxy

import (
	"time"

	"github.com/zuoyebang/bitalostored/butils/math2"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
)

func startMetricsReporter(p *Proxy, d time.Duration, do func() error) {
	go func() {
		var ticker = time.NewTicker(d)
		defer ticker.Stop()
		var delay = &utils.DelayExp2{
			Min: 1, Max: 15,
			Unit: time.Second,
		}
		for !p.IsClosed() {
			<-ticker.C
			if err := do(); err != nil {
				log.Errorf("report metrics failed err:%v", err)
				delay.SleepWithCancel(p.IsClosed)
			} else {
				delay.Reset()
			}
		}
	}()
}

func (p *Proxy) startMetricsExporter(cfg *config.Config) {
	if cfg.MetricsReportLogSwitch == 0 {
		return
	}

	period := cfg.MetricsReportLogPeriod.Duration()
	period = math2.MaxDuration(time.Second, period)

	var doMetricsExporterNum int
	startMetricsReporter(p, period, func() error {
		stats := GetStats(p, true)
		fields := map[string]interface{}{
			"cdm_ops_total":            stats.CmdOps.Total,
			"cdm_ops_fails":            stats.CmdOps.Fails,
			"cdm_ops_periodfails":      stats.CmdOps.PeriodFails,
			"cdm_ops_qps":              stats.CmdOps.QPS,
			"cmd_cost_avg":             stats.CmdOps.AvgCost,
			"cmd_cost_kv":              stats.CmdOps.KVCost,
			"cmd_cost_list":            stats.CmdOps.ListCost,
			"cmd_cost_hash":            stats.CmdOps.HashCost,
			"cmd_cost_set":             stats.CmdOps.SetCost,
			"cmd_cost_zset":            stats.CmdOps.ZsetCost,
			"cmd_cost_write":           stats.CmdOps.WriteCost,
			"cmd_cost_read":            stats.CmdOps.ReadCost,
			"sessions_total":           stats.Sessions.Total,
			"sessions_alive":           stats.Sessions.Alive,
			"rusage_mem":               stats.Rusage.Mem,
			"rusage_cpu":               stats.Rusage.CPU,
			"runtime_gc_num":           stats.Runtime.GC.Num,
			"runtime_gc_total_pausems": int64(stats.Runtime.GC.TotalPauseMs),
			"runtime_num_procs":        stats.Runtime.NumProcs,
			"runtime_num_goroutines":   stats.Runtime.NumGoroutines,
			"runtime_num_cgo_call":     stats.Runtime.NumCgoCall,
		}
		log.Stats(fields)
		doMetricsExporterNum++
		if doMetricsExporterNum >= cfg.MetricsResetCycle {
			doMetricsExporterNum = 0
			dostats.PeriodResetStats()
		}
		return nil
	})
}
