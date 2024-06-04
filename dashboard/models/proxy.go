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

package models

import "github.com/zuoyebang/bitalostored/butils/timesize"

type Proxy struct {
	Id         int    `json:"id,omitempty"`
	Token      string `json:"token"`
	VersionTag string `json:"version_tag"`
	StartTime  string `json:"start_time"`
	AdminAddr  string `json:"admin_addr"`

	ProtoType string `json:"proto_type"`
	ProxyAddr string `json:"proxy_addr"`

	ProductName string `json:"product_name"`
	CloudType   string `json:"cloudtype"`

	Pid int    `json:"pid"`
	Pwd string `json:"pwd"`
	Sys string `json:"sys"`

	Hostname  string         `json:"hostname"`
	HostPort  string         `json:"hostport"`
	RedisConf *RedisConnConf `json:"redis_conf"`
}

type RedisConnConf struct {
	HostPort     string            `toml:"host_port" json:"host_port,omitempty"`
	MaxIdle      int               `toml:"max_idle" json:"max_idle"`
	MaxActive    int               `toml:"max_active" json:"max_active"`
	IdleTimeout  timesize.Duration `toml:"idle_timeout" json:"idle_timeout"`
	ConnLifeTime timesize.Duration `toml:"conn_lifetime" json:"conn_lifetime"`
	Password     string            `toml:"password" json:"password"`
	DataBase     int               `toml:"database" json:"database"`
	ConnTimeout  timesize.Duration `toml:"conn_timeout" json:"conn_timeout"`
	ReadTimeout  timesize.Duration `toml:"read_timeout" json:"read_timeout"`
	WriteTimeout timesize.Duration `toml:"write_timeout" json:"write_timeout"`
}

func (p *Proxy) Encode() []byte {
	return jsonEncode(p)
}
