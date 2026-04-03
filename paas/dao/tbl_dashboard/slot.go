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

package tbl_dashboard

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type Slot struct {
	Id int `json:"id"`

	MasterAddr        string `json:"master_addr"`
	MasterAddrGroupId int    `json:"master_addr_group_id"`

	LocalCloudServers  []string `json:"local_servers"`
	BackupCloudServers []string `json:"backup_servers"`
	WitnessServers     []string `json:"witness_servers"`

	GroupServersCloudMap map[string]string `json:"group_servers_cloudmap"`
	GroupServersStats    map[string]bool   `json:"group_servers_stats"`
}

type SlotMapping struct {
	Id      int  `json:"id"`
	GroupId uint `json:"group_id"`
}

type Proxy struct {
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

	Hostname string `json:"hostname"`
	HostPort string `json:"hostport"`
}

func GetAllProxy(clusterName string) (map[string][]*Proxy, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		log.Warn("failed to connect tblDashboard.err:", err)
		return nil, err
	}
	db = db.Where("product_name = ? and sub_path = ?", clusterName, "proxy")
	res := make([]*Dashboard, 0)
	db.Find(&res)
	if db.Error != nil {
		return nil, db.Error
	}
	ret := make(map[string][]*Proxy)
	for _, dh := range res {
		sm := &Proxy{}
		e := jsoniter.UnmarshalFromString(dh.Value, sm)
		if e != nil {
			log.Errorf("unmarshall err:%v", e)
		}
		if _, ok := ret[sm.CloudType]; !ok {
			ret[sm.CloudType] = make([]*Proxy, 0)
		}
		ret[sm.CloudType] = append(ret[sm.CloudType], sm)
	}
	return ret, nil
}

func GetAllSlots(clusterName string) (map[uint][]*SlotMapping, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		log.Warn("failed to connect tblDashboard.err:", err)
		return nil, err
	}
	db = db.Where("product_name = ? and sub_path = ?", clusterName, "slots")
	res := make([]*Dashboard, 0)
	db.Find(&res)
	if db.Error != nil {
		return nil, db.Error
	}
	ret := make(map[uint][]*SlotMapping)
	for _, dh := range res {
		sm := &SlotMapping{}
		e := jsoniter.UnmarshalFromString(dh.Value, sm)
		if e != nil {
			log.Errorf("unmarshall err:%v", e)
		}
		if _, ok := ret[sm.GroupId]; !ok {
			ret[sm.GroupId] = make([]*SlotMapping, 0)
		}
		ret[sm.GroupId] = append(ret[sm.GroupId], sm)
	}
	return ret, nil
}
