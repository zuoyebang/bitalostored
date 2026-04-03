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

package tbl_service

import (
	"encoding/json"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"time"
)

const TableName = "tblService"

type Service struct {
	ID               uint   `gorm:"column:id;primary_key" json:"serviceId"`
	Name             string `gorm:"column:name" json:"serviceName"`
	PortRange        string `gorm:"column:port_range" json:"-"`
	ClusterPortRange string `gorm:"column:cluster_port_range" json:"-"`

	ClusterPortRanges []int `gorm:"-" json:"clusterPortRange"`
	PortRanges        []int `gorm:"-" json:"portRange"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetList(limit int, offset int) ([]*Service, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if limit >= 0 {
		db = db.Limit(limit).Offset(offset)
	}

	res := []*Service{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	for _, v := range res {
		err = json.Unmarshal([]byte(v.PortRange), &v.PortRanges)
		if err != nil {
			log.Error("get servicelist error.err:", err, " ", v)
		}
		err = json.Unmarshal([]byte(v.ClusterPortRange), &v.ClusterPortRanges)
		if err != nil {
			log.Error("get servicelist error.err:", err, " ", v)
		}
	}
	return res, db.Error
}

func GetListByIds(ids []uint) (map[uint]*Service, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("id in ?", ids)

	var res []*Service
	db.Find(&res)
	ret := make(map[uint]*Service, len(res))
	for _, re := range res {
		err = json.Unmarshal([]byte(re.PortRange), &re.PortRanges)
		if err != nil {
			log.Error("get servicelist error.err:", err, " ", re)
		}
		err = json.Unmarshal([]byte(re.ClusterPortRange), &re.ClusterPortRanges)
		if err != nil {
			log.Error("get servicelist error.err:", err, " ", re)
		}
		ret[re.ID] = re
	}
	return ret, db.Error
}

func GetInfo(id uint) (*Service, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("id = ?", id)

	res := &Service{}
	db.First(res)
	json.Unmarshal([]byte(res.PortRange), &res.PortRanges)
	json.Unmarshal([]byte(res.ClusterPortRange), &res.ClusterPortRanges)
	return res, db.Error
}

func GetInfoByName(name string) (*Service, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("name = ?", name)

	res := &Service{}
	db.First(res)
	json.Unmarshal([]byte(res.PortRange), &res.PortRanges)
	json.Unmarshal([]byte(res.ClusterPortRange), &res.ClusterPortRanges)
	return res, db.Error
}
func Create(name string, portRange, clusterPortRange []int) (uint, error) {
	clusterPort, e := json.Marshal(clusterPortRange)
	if e != nil {
		return 0, e
	}
	port, e := json.Marshal(portRange)
	if e != nil {
		return 0, e
	}

	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}

	res := &Service{
		Name:             name,
		PortRange:        string(port),
		ClusterPortRange: string(clusterPort),
		CreateTime:       time.Now().Unix(),
		UpdateTime:       time.Now().Unix(),
	}
	db.Create(res)
	return res.ID, db.Error
}

func UpdateTime() error {
	now := time.Now().Unix()
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db.Where("id = 1").UpdateColumn("update_time", now)
	return db.Error
}
