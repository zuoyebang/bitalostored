// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"

	"github.com/BurntSushi/toml"
)

var defaultConf *PaaSConf

func GetConf() PaaSConf {
	return *defaultConf
}

func GetAuth(clusterId uint, clusterName string) string {
	for _, auths := range defaultConf.RedisAuths {
		if auths.ClusterId == int(clusterId) || auths.ClusterName == clusterName {
			return auths.AdminAuth
		}
	}
	return ""
}

func SetConf(configPath string) error {
	_, err := toml.DecodeFile(configPath, &defaultConf)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("config detail:%+v", defaultConf)
	return defaultConf.Validate()
}
