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

const DefaultConfig = `
##################################################
#                                                #
#                  Bitalos-Dashboard               #
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

type Config struct {
	CoordinatorName string `toml:"coordinator_name" json:"coordinator_name"`
	CoordinatorAddr string `toml:"coordinator_addr" json:"coordinator_addr"`
	CoordinatorAuth string `toml:"coordinator_auth" json:"coordinator_auth"`

	AdminAddr  string `toml:"admin_addr" json:"admin_addr"`
	AdminModel string `toml:"admin_model" json:"admin_model"`

	HostAdmin string `toml:"-" json:"-"`
	PaasHost  string `toml:"paas_host" json:"paas_host"`
	DhRefer   string `toml:"dh_refer" json:"dh_refer"`
	Area      string `toml:"area" json:"area"`
	Env       string `toml:"env" json:"env"`

	ReadCrossCloud int `toml:"read_cross_cloud" json:"read_cross_cloud"`

	ProductName string       `toml:"product_name" json:"product_name"`
	ProductAuth string       `toml:"product_auth" json:"product_auth"`
	Cloud       string       `toml:"cloud" json:"cloud"`
	Database    DBConfig     `toml:"database"`
	Stored      StoredConfig `toml:"stored"`
}

type StoredConfig struct {
	Txcloud []string `toml:"txcloud" json:"txcloud"`
	Ali     []string `toml:"ali" json:"ali"`
	Baidu   []string `toml:"baidu" json:"baidu"`
}

type DBConfig struct {
	Username string `toml:"username"`
	Password string `toml:"password"`
	HostPort string `toml:"hostport"`
	DBName   string `toml:"dbname"`
}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		log.PanicErrorf(err, "decode toml failed")
	}
	// if err := c.Validate(); err != nil {
	// 	log.PanicErrorf(err, "validate config failed")
	// }
	return c
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
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
	if c.CoordinatorName == "" {
		return errors.New("invalid coordinator_name")
	}
	if c.CoordinatorAddr == "" {
		return errors.New("invalid coordinator_addr")
	}
	if c.AdminAddr == "" {
		return errors.New("invalid admin_addr")
	}
	if c.ProductName == "" {
		return errors.New("invalid product_name")
	}
	if c.AdminModel != "raft" {
		return errors.New("invalid admin model raft")
	}
	return nil
}
