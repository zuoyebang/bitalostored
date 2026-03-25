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

package rpc_test

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/rpc"
	"testing"
)

func TestMain(m *testing.M) {
	err := config.SetConf("/Users/bitlos/git/paas/storedpaas/conf/storedpaas_online_tx.toml")
	fmt.Println(err)
	m.Run()
}

func TestSendDingding(t *testing.T) {
	msg := "sdfdsf"
	rpc.SendDingding(rpc.OpErrTitle, msg)
}
