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

package mdl_node

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"testing"
)

func TestMain(m *testing.M) {
	configPath := "/Users/git/paas/storedpaas/conf/storedpaas_dev.toml"
	err := config.SetConf(configPath)
	if err != nil {
		log.Errorf("read config file failed.err:%+v", err)
		return
	}

	if err := dao.InitDB(config.GetConf().Database); err != nil {
		log.Errorf("open database failed.err:%+v", err)
		return
	}
	log.NewLogger(&log.Options{
		IsDebug:      false,
		RotationTime: "Daily",
		LogPath:      "/Users/git/paas/storedpaas/log/storedpaas",
	})
	m.Run()
}
