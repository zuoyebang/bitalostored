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

package model_test

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/model"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"testing"

	jsoniter "github.com/json-iterator/go"
)

// go test -v api_test.go -test.run TestGtQuestion

func TestMain(m *testing.M) {
	configPath := "../conf/storedpaas_dev.toml"
	err := config.SetConf(configPath)
	//err = config.InitConfig(configPath)
	if err != nil {
		log.Errorf("read config file failed.err:%+v", err)
		return
	}

	if err := dao.InitDB(config.GetConf().Database); err != nil {
		log.Errorf("open database failed.err:%+v", err)
		return
	}
	m.Run()
}

func TestNodeCreateWitness(t *testing.T) {
	cid := uint(181)
	fmt.Println(cid)
	cosFileId := uint(1613)
	idc := "txcloud"
	_ = idc
	_ = cosFileId
	mdl_node.CreateAllWitness(cid, cosFileId, idc)
}

func TestInitDatabase(t *testing.T) {
	// Import initial data to database
	if err := model.Init(); err != nil {
		log.Fatal("init model data failed")
	}
}

func TestNoRecord(t *testing.T) {
	clusterId := uint(181)
	groupId := uint(35)
	machineId := uint(51) // 51=true 50=false
	// No record return nil, not error
	fmt.Println(tbl_node.CheckMachineDuplicatedByGroup(clusterId, groupId, machineId))
}

func TestMaxVersion(t *testing.T) {
	serviceId := def.SERVICE_ID_BITALOS
	fmt.Println(tbl_cosfile.GetMaxVersion(uint(serviceId)))
}

func TestGetPort(t *testing.T) {
	fmt.Println(tbl_hostport.GetIdlePortList(8200, 8900, 3))

	region, err := tbl_region.GetInfo(13)
	if err != nil {
		t.Error(err)
	}
	machineList, err := mdl_cluster.FindRegionIp(region, 3)
	if err != nil {
		t.Error(err)
	}
	for _, m := range machineList {
		fmt.Println(m.IP)
	}
}

func TestGetNodeIdByAddress(t *testing.T) {
	nodeList := []string{"127.0.0.1:232"}
	var groupId uint = 2
	var clusterId uint = 59
	ret, err := mdl_node.GetNodeIdByAddress(nodeList, groupId, clusterId)
	if err != nil {
		t.Errorf("GetNodeIdByAddress err %v", err)
	}
	nodeIdListStr, _ := jsoniter.Marshal(ret)
	t.Logf("[GetNodeIdByAddress] data= %+v, res= %s", ret, string(nodeIdListStr))
}
