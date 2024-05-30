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

package config

import (
	"bytes"
	"errors"
	"time"

	"github.com/zuoyebang/bitalostored/butils/bytesize"
	"github.com/zuoyebang/bitalostored/butils/timesize"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/internal/switcher"

	"github.com/BurntSushi/toml"
)

const DefaultConfig = `
product_name = "stored-demo"
product_auth = ""

proxy_auth_enabled = false
proxy_auth_password = "storedproxy.clustername"
proxy_auth_admin = "storedproxy.clustername.admin"

admin_addr = "0.0.0.0:8111"

proto_type = "tcp4"
proxy_addr = "0.0.0.0:8112"

dashboard_proto_type = "http"
dashboard_username = "username"
dashboard_password = "password"

proxy_cloudtype = "baidu"

proxy_max_clients = 1000
conn_read_buffersize = "4kb"
conn_write_buffersize = "4kb"

pprof_switch = 0
pprof_address = ":8113"

metrics_report_log_switch = 0
metrics_report_log_period = "1s"

[log]
is_debug = false
rotation_time = "Hourly"
log_file = "/tmp/proxy.log"
access_log = false
access_log_file = "/tmp/proxy.access.log"
slow_log = true
slow_log_cost = "30ms"
slow_log_file = "/tmp/proxy.slow.log"

[redis_default_conf]
max_idle = 50
max_active = 50
idle_timeout = "3600s"
conn_lifetime = "3600s"
password = ""
database = 0
conn_timeout = "50ms" 
read_timeout = "500ms"
write_timeout = "500ms"

# client session deadline
[dynamic_deadline]
# the ratio of alive client to max client
client_ratio_threshold = [0,30,60,80,90]
deadline_threshold = ["180s","100s","30s","6s","2s"]
`

const (
	CrossCloudOverwrite = 0
	CrossCloudEnable    = 1
	CrossCloudDisable   = 2
)

type DynamicDeadline struct {
	ClientRatios      []int               `toml:"client_ratio_threshold" json:"client_ratio_threshold"`
	DeadlineThreshold []timesize.Duration `toml:"deadline_threshold" json:"deadline_threshold"`
}

func (dd DynamicDeadline) Validate() error {
	if len(dd.ClientRatios) != len(dd.DeadlineThreshold) {
		return errors.New("length of array client_ratio_threshold and deadline_threshold should be equal")
	}
	ratio := 0
	for _, r := range dd.ClientRatios {
		if r > 100 || r < 0 {
			return errors.New("the ratio of client_ratio_threshold should be in [0,100]")
		}
		if r < ratio {
			return errors.New("the ratio of client_ratio_threshold should be increasing")
		}
		ratio = r
	}
	deadline := timesize.Duration(3 * 86400 * time.Second)
	for _, t := range dd.DeadlineThreshold {
		if t.Int64() > timesize.Duration(3*86400*time.Second).Int64() || t <= 0 {
			return errors.New("the deadline configure is unreasonable")
		}
		if t.Int64() > deadline.Int64() {
			return errors.New("the deadline configure should be decline")
		}
		deadline = t
	}
	return nil
}

type LogConfig struct {
	IsDebug       bool              `toml:"is_debug" json:"is_debug"`
	RotationTime  string            `toml:"rotation_time" json:"rotation_time"`
	LogFile       string            `toml:"log_file" json:"log_file"`
	StatsLogFile  string            `toml:"stats_log_file" json:"stats_log_file"`
	SlowLog       bool              `toml:"slow_log" json:"slow_log"`
	SlowLogFile   string            `toml:"slow_log_file" json:"slow_log_file"`
	SlowLogCost   timesize.Duration `toml:"slow_log_cost" json:"slow_log_cost"`
	AccessLog     bool              `toml:"access_log" json:"access_log"`
	AccessLogFile string            `toml:"access_log_file" json:"access_log_file"`
}

func (l LogConfig) Validate() error {
	return nil
}

type Config struct {
	ProductName         string         `toml:"product_name" json:"product_name"`
	ProductAuth         string         `toml:"product_auth" json:"-"`
	ProxyAuthEnabled    bool           `toml:"proxy_auth_enabled" json:"proxy_auth_enabled"`
	ProxyAuthPassword   string         `toml:"proxy_auth_password" json:"proxy_auth_password"`
	ProxyAuthAdmin      string         `toml:"proxy_auth_admin" json:"proxy_auth_admin"`
	ProtoType           string         `toml:"proto_type" json:"proto_type"`
	ProxyAddr           string         `toml:"proxy_addr" json:"proxy_addr"`
	AdminAddr           string         `toml:"admin_addr" json:"admin_addr"`
	DashboardProtoType  string         `toml:"dashboard_proto_type" json:"dashboard_proto_type"`
	DashboardUsername   string         `toml:"dashboard_username" json:"dashboard_username"`
	DashboardPassword   string         `toml:"dashboard_password" json:"dashboard_password"`
	HostProxy           string         `toml:"-" json:"-"`
	HostAdmin           string         `toml:"-" json:"-"`
	ProxyMaxClients     int            `toml:"proxy_max_clients" json:"proxy_max_clients"`
	MaxProcs            int            `toml:"max_procs" json:"max_procs"`
	ConnReadBufferSize  bytesize.Int64 `toml:"conn_read_buffersize" json:"conn_read_buffersize"`
	ConnWriteBufferSize bytesize.Int64 `toml:"conn_write_buffersize" json:"conn_write_buffersize"`
	ReadOnlyProxy       bool           `toml:"proxy_read_only" json:"proxy_read_only"`
	ProxyCloudType      string         `toml:"proxy_cloudtype" json:"proxy_cloudtype"`
	ReadCrossCloud      int            `toml:"read_cross_cloud" json:"read_cross_cloud"`
	OpenDistributedTx   bool           `toml:"open_distributed_tx" json:"open_distributed_tx"`

	PprofSwitch  int    `toml:"pprof_switch" json:"pprof_switch"`
	PprofAddress string `toml:"pprof_address" json:"pprof_address"`

	BreakerStopTimeout    timesize.Duration `toml:"breaker_stop_timeout" json:"breaker_stop_timeout"`
	BreakerOpenFailRate   float64           `toml:"breaker_open_fail_rate" json:"breaker_open_fail_rate"`
	BreakerRestoreRequest int               `toml:"breaker_restore_request" json:"breaker_restore_request"`

	MetricsReportLogSwitch int               `toml:"metrics_report_log_switch" json:"metrics_report_log_switch"`
	MetricsReportLogPeriod timesize.Duration `toml:"metrics_report_log_period" json:"metrics_report_log_period"`
	MetricsResetCycle      int               `toml:"metrics_reset_cycle" json:"metrics_reset_cycle"`

	ReadMasterChance int `toml:"read_master_chance" json:"read_master_chance"`

	Log LogConfig `toml:"log" json:"log"`

	RedisDefaultConf models.RedisConnConf `json:"redis_default_conf"`

	DynamicDeadline DynamicDeadline `toml:"dynamic_deadline" json:"dynamic_deadline"`
}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		log.Fatalf("config decode toml failed err:%s", err.Error())
	}
	if err := c.Validate(); err != nil {
		log.Fatalf("config validate failed err:%s", err.Error())
	}
	return c
}

func (c *Config) LoadFromFile(path string) error {
	if _, err := toml.DecodeFile(path, c); err != nil {
		return err
	}
	return c.Validate()
}

func (c *Config) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}

func (c *Config) Validate() error {
	if c.ProtoType == "" {
		return errors.New("invalid proto_type")
	}
	if c.ProxyAddr == "" {
		return errors.New("invalid proxy_addr")
	}
	if c.AdminAddr == "" {
		return errors.New("invalid admin_addr")
	}
	if c.ProductName == "" {
		return errors.New("invalid product_name")
	}
	if c.ProxyCloudType == "" {
		return errors.New("invalid product_name")
	}

	if c.ProxyMaxClients < 0 {
		return errors.New("invalid proxy_max_clients")
	}

	if c.MaxProcs <= 0 {
		c.MaxProcs = 4
	}

	const MaxInt = bytesize.Int64(^uint(0) >> 1)

	if c := c.ConnReadBufferSize; c < 0 || c > MaxInt {
		return errors.New("invalid conn_read_buffersize")
	}
	if c := c.ConnWriteBufferSize; c < 0 || c > MaxInt {
		return errors.New("invalid conn_write_buffersize")
	}

	if c.MetricsReportLogPeriod.Int64() < 0 {
		return errors.New("invalid metrics_report_log_period")
	}

	if c.MetricsResetCycle <= 0 {
		c.MetricsResetCycle = 600
	}

	if c.RedisDefaultConf.MaxIdle < 0 {
		return errors.New("invalid max_idel")
	}

	if c.RedisDefaultConf.MaxActive < 0 || c.RedisDefaultConf.MaxActive < c.RedisDefaultConf.MaxIdle {
		return errors.New("invalid max_active")
	}

	if c.RedisDefaultConf.DataBase < 0 || c.RedisDefaultConf.DataBase > 7 {
		return errors.New("invalid database")
	}
	if c.RedisDefaultConf.ConnTimeout < 0 {
		return errors.New("invalid conn_timeout")
	}
	if c.RedisDefaultConf.ReadTimeout < 0 {
		return errors.New("invalid read_timeout")
	}
	if c.RedisDefaultConf.WriteTimeout < 0 {
		return errors.New("invalid write_timeout")
	}
	if c.RedisDefaultConf.ConnLifeTime < 0 {
		return errors.New("invalid conn_lifetime")
	}

	if len(c.Log.LogFile) < 0 {
		return errors.New("invaild log_file")
	}
	if len(c.Log.StatsLogFile) < 0 {
		return errors.New("invaild stat_log_file")
	}
	if c.Log.AccessLog && len(c.Log.AccessLogFile) < 0 {
		return errors.New("invaild access log conf")
	}
	if c.Log.SlowLog && len(c.Log.SlowLogFile) < 0 {
		return errors.New("invaild slow log conf")
	}
	if c.Log.SlowLogCost < timesize.Duration(20*time.Millisecond) {
		c.Log.SlowLogCost = timesize.Duration(20 * time.Millisecond)
	}
	if !log.CheckRotation(c.Log.RotationTime) {
		c.Log.RotationTime = log.HourlyRotate
	}

	if c.ReadCrossCloud == CrossCloudEnable {
		switcher.ReadCrossCloud.Store(true)
	} else if c.ReadCrossCloud == CrossCloudDisable {
		switcher.ReadCrossCloud.Store(false)
	}

	if c.BreakerOpenFailRate <= 0.0 || c.BreakerOpenFailRate > 0.05 {
		c.BreakerOpenFailRate = 0.05
	}
	if c.BreakerRestoreRequest <= 0 {
		c.BreakerRestoreRequest = 50
	}
	if c.BreakerStopTimeout.Duration() == 0 {
		c.BreakerStopTimeout = timesize.Duration(200 * time.Millisecond)
	}

	if err := c.DynamicDeadline.Validate(); err != nil {
		return err
	}

	if c.ReadMasterChance <= 50 || c.ReadMasterChance > 100 {
		c.ReadMasterChance = 90
	}
	return nil
}
