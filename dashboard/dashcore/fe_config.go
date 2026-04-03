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

package dashcore

import (
	"bytes"

	"github.com/BurntSushi/toml"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
)

const DefaultFEConfig = `
##################################################
#                                                #
#                  Bitalos-Fe               #
#                                                #
##################################################

# Set Coordinator, only accept "sqlite" & "database".
# for sqlite, coorinator_auth accept "user:password" 
# Quick Start
coordinator_name = ""
coordinator_addr = ""
coordinator_auth = ""

# Set Stored Product Name/Auth.
product_name = ""
product_auth = ""

# Set bind address for admin(rpc), tcp only.
admin_addr = "0.0.0.0:18080"
# Set Stored raft
admin_model  = "raft"
`

type FEConfig struct {
	Ncpu       int      `toml:"ncpu" json:"ncpu"`
	Env        string   `toml:"env" json:"env"`
	WhiteUsers []string `toml:"white_users" json:"whiteUsers"`

	CheckHost  string `toml:"check_host" json:"checkHost"`
	CheckRefer string `toml:"check_refer" json:"checkRefer"`
	GrafanaUrl string `toml:"grafana_url" json:"grafanaUrl"`
	PaasDomain string `toml:"paas_domain" json:"paasDomain"`
	MainUrl    string `toml:"main_url" json:"mainUrl"`

	DbType   string       `toml:"db" json:"db"`
	Sqlite   string       `toml:"sqlite" json:"sqlite"`
	Database FEDBConfig   `toml:"database"`
	Ips      IpsConf      `toml:"ips"`
	Cloud    string       `toml:"cloud" json:"cloud"`
	Stored   StoredConfig `toml:"stored" json:"stored"`
}

type IpsConf struct {
	AppId  string `toml:"appId" json:"appId"`
	Secret string `toml:"secret" json:"secret"`
}

func NewDefaultFEConfig() *FEConfig {
	c := &FEConfig{}
	if _, err := toml.Decode(DefaultFEConfig, c); err != nil {
		log.PanicErrorf(err, "decode toml failed")
	}
	if err := c.Validate(); err != nil {
		log.PanicErrorf(err, "validate config failed")
	}
	return c
}

func (c *FEConfig) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	return c.Validate()
}

func (c *FEConfig) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}

func (c *FEConfig) Validate() error {
	if c.Ncpu <= 0 {
		c.Ncpu = 2
	}
	return nil
}

type FEDBConfig struct {
	Username string `toml:"username"`
	Password string `toml:"password"`
	HostPort string `toml:"hostport"`
	DBName   string `toml:"dbname"`
}
