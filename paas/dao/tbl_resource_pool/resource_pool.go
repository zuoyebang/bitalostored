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

package tbl_resource_pool

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"time"
)

const TableName = "tblResourcePool"

type Resource struct {
	ID           uint   `gorm:"column:id" json:"id"`
	ClusterName  string `gorm:"column:cluster_name" json:"clusterName"`
	ClusterId    uint   `gorm:"column:cluster_id" json:"clusterId"`
	ServiceId    uint   `gorm:"column:service_id" json:"serviceId"`
	Port         uint   `gorm:"column:port" json:"port"`
	CpuSetType   int    `gorm:"column:cpu_set_type" json:"cpuSetType"`
	IDC          string `gorm:"column:idc" json:"idc"`
	MetricName   string `gorm:"column:metric_name" json:"metricName"`
	CgroupLimit  int64  `gorm:"column:cgroup_limit" json:"cgroupLimit"`
	SuggestValue int64  `gorm:"column:suggest_value" json:"suggestValue"`
	ManualValue  int64  `gorm:"column:manual_value" json:"manualValue"`
	CostValue    int64  `gorm:"column:cost_value" json:"costValue"`
	MaxCpu       int64  `gorm:"column:max_cpu" json:"maxCpu"`
	MinCpu       int64  `gorm:"column:min_cpu" json:"minCpu"`
	SyncTime     int64  `gorm:"column:sync_time" json:"-"`
	SyncDate     string `gorm:"-" json:"syncTime"`
	ApplyTime    int64  `gorm:"column:apply_time" json:"-"`
	ApplyDate    string `gorm:"-" json:"applyTime"`
	CreateTime   int64  `gorm:"column:create_time" json:"-"`
	CreateDate   string `gorm:"-" json:"createTime"`
	UpdateTime   int64  `gorm:"column:update_time" json:"-"`
	UpdateDate   string `gorm:"-" json:"updateTime"`
}

func GetResourceList(clusterName, idc string, serviceId, isManual, cpuSetType int) ([]*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if len(clusterName) > 0 {
		db = db.Where("cluster_name = ?", clusterName)
	}
	if idc != "all" {
		db = db.Where("idc = ?", idc)
	}
	if serviceId > 0 {
		db = db.Where("service_id = ?", serviceId)
	}
	if isManual == 1 {
		db = db.Where("suggest_value = cgroup_limit")
	}
	if isManual == 2 {
		db = db.Where("suggest_value != cgroup_limit")
	}
	if isManual == 3 {
		db = db.Where("manual_value > 0")
	}
	if cpuSetType >= 0 {
		db = db.Where("cpu_set_type = ?", cpuSetType)
	}

	var res []*Resource
	db = db.Find(&res)
	for _, r := range res {
		r.SyncDate = math2.UnixTimeToStr(r.SyncTime)
		r.ApplyDate = math2.UnixTimeToStr(r.ApplyTime)
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetNodeResource(clusterId uint, idc string) ([]*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and idc = ?", clusterId, idc)
	var res []*Resource
	db = db.Find(&res)
	return res, db.Error
}

func GetSingleResourceCpu(clusterId uint, idc string) ([]*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and idc = ? and metric_name = 'cpu'", clusterId, idc)
	var res []*Resource
	db = db.Find(&res)
	return res, db.Error
}

func GetNeedApplyResource(serviceId uint) ([]*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if serviceId > 0 {
		db = db.Where("suggest_value > 0 and cgroup_limit != suggest_value and service_id = ?", serviceId)
	} else {
		db = db.Where("suggest_value > 0 and cgroup_limit != suggest_value")
	}
	var res []*Resource
	db = db.Find(&res)
	return res, db.Error
}

func GetAllResource() ([]*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Resource
	db = db.Find(&res)
	return res, db.Error
}

func GetResourceByIds(ids []int) ([]*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("id in (?)", ids)
	var res []*Resource
	db = db.Find(&res)
	return res, db.Error
}

func Create(clusterName, metricName, idc string, clusterId, serviceId, port uint, cgroupLimit int64) (*Resource, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	res := &Resource{
		ClusterId:   clusterId,
		ServiceId:   serviceId,
		Port:        port,
		IDC:         idc,
		ClusterName: clusterName,
		MetricName:  metricName,
		CgroupLimit: cgroupLimit,
		CostValue:   cgroupLimit,
		CpuSetType:  def.NOT_SET_CPU,
		CreateTime:  now,
	}
	db = db.Create(res)
	return res, db.Error
}

func Update(id uint, resource *Resource) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	resource.UpdateTime = time.Now().Unix()
	db = db.First(&Resource{}, id).UpdateColumns(resource)
	return db.Error
}

func UpdateCpuSet(id int, cpuSet int) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	db = db.Where("id = ?", id).Updates(map[string]interface{}{"cpu_set_type": cpuSet, "update_time": now})
	return db.Error
}

func Apply(id uint, resource *Resource) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	db = db.Exec("update tblResourcePool set cgroup_limit = suggest_value, apply_time = ? where id = ?", resource.ApplyTime, id)
	return db.Error
}

func Applys(ids []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	db = db.Exec("update tblResourcePool set cgroup_limit = suggest_value, apply_time = ?  where id in (?)",
		now, ids)
	return db.Error
}

func ApplyManual(ids []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	db = db.Exec("update tblResourcePool set cgroup_limit = manual_value, apply_time = ?  where id in (?)",
		now, ids)
	return db.Error
}

func ApplyManualTmp(ids []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	db = db.Exec("update tblResourcePool set cgroup_limit = manual_value, manual_value = 0, apply_time = ?  where id in (?)",
		now, ids)
	return db.Error
}

func UpdateCpuBySelection(resource *Resource) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	affected := db.Where("cluster_id = ? and service_id = ? and idc = ? and metric_name = ?", resource.ClusterId, resource.ServiceId,
		resource.IDC, resource.MetricName).Updates(map[string]interface{}{"suggest_value": resource.SuggestValue, "sync_time": now}).RowsAffected
	if affected <= 0 {
		resource.CreateTime = now
		resource.SyncTime = now
		db = db.Create(resource)
	}

	return db.Error
}

func UpdateManualById(manualValue, minCpu, maxCpu int64, id uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	db = db.Where("id = ?", id).Updates(map[string]interface{}{"manual_value": manualValue, "min_cpu": minCpu, "max_cpu": maxCpu, "update_time": now})
	return db.Error
}

func UpdateCostById(cpu int64, id uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	db = db.Where("id = ?", id).Updates(map[string]interface{}{"cost_value": cpu, "update_time": now})
	return db.Error
}
