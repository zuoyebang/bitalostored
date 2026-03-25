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

package tbl_task

import (
	"encoding/json"
	"errors"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const TableName = "tblTask"

type Task struct {
	ID     uint   `gorm:"column:id;primary_key" json:"taskId"`
	Type   string `gorm:"column:type" json:"taskType"`
	Status string `gorm:"column:status" json:"taskStatus"`

	RegionId       uint   `gorm:"column:region_id" json:"regionId"`
	MachineId      uint   `gorm:"column:machine_id" json:"machineId"`
	ServiceId      uint   `gorm:"column:service_id" json:"serviceId"`
	CosFileId      uint   `gorm:"column:cos_file_id" json:"cosFileId"`
	CosFileVersion string `gorm:"column:cos_file_version" json:"cosFileVersion"`

	ClusterId uint `gorm:"column:cluster_id" json:"clusterId"`
	GroupId   uint `gorm:"column:group_id" json:"groupId"`
	NodeId    uint `gorm:"column:node_id" json:"nodeId"`

	Extra   string    `gorm:"column:extra;type:text" json:"-"`
	TaskExt TaskExtra `gorm:"-" json:"taskExt"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

type TaskExtra struct {
	Ip               string `json:"ip"`
	RegionName       string `json:"regionName"`
	ServiceName      string `json:"serviceName"`
	HostName         string `json:"hostName"`
	ServicePort      uint   `json:"servicePort"`
	ServicePortRange []int  `json:"servicePortRange"`
	ClusterPortRange []int  `json:"clusterPortRange"`
	ClusterPort      uint   `json:"clusterPort"`
	ClusterName      string `json:"clusterName"`
	MigratedFromInfo string `json:"migrateFromInfo"`
	CloudType        string `json:"cloudType"`
	DashboardName    string `json:"dashboardName"`
	DashboardAddress string `json:"dashboardAddress"`
	Operation        string `json:"operation"`
	StoredAuth       string `json:"storedAuth"`
	DeraftToken      string `json:"deraftToken"`
	UpdateConfig     bool   `json:"updateConfig"`

	TargetGroupId uint `json:"targetGroupId"`

	ExtString string `json:"extString"`

	NodeList    []string `json:"nodeList"`
	NodeListStr string
	NodeListVal string
	NodeIdList  string
	NodeIndex   int  `json:"nodeIndex"`
	IsWitness   bool `json:"isWitness"`
	IsObserver  bool `json:"isObserver"`
}

func GetList(machineId uint, types []string, status []string, limit int, offset int) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if len(types) == 0 {
		db = db.Where("machine_id = ? and status in (?)", machineId, status)
	} else {
		db = db.Where("machine_id = ? and type in (?) and status in (?)", machineId, types, status)
	}
	db = db.Limit(limit).Offset(offset)

	var res []*Task
	db = db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	for _, v := range res {
		json.Unmarshal([]byte(v.Extra), &v.TaskExt)
	}
	return res, db.Error
}

func GetServerTask() ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("service_id in (?)", []uint{def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX})
	var res []*Task
	db = db.Find(&res)
	if db.Error != nil {
		log.Warnf("GetServerTask err:%v", db.Error)
	}
	return res, db.Error
}

func GetHistoryList(clusterId, groupId, nodeId uint, page, num int) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and group_id = ? and node_id = ?", clusterId, groupId, nodeId)
	db = db.Order("create_time desc").Offset((page - 1) * num).Limit(num)
	var res []*Task
	db = db.Find(&res)
	for _, v := range res {
		_ = json.Unmarshal([]byte(v.Extra), &v.TaskExt)
	}
	return res, db.Error
}

func GetHistoryCount(clusterId, groupId, nodeId uint) int64 {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0
	}
	var count int64
	db = db.Where("cluster_id = ? and group_id = ? and node_id = ?", clusterId, groupId, nodeId).Count(&count)
	return count
}

func GetInitRaftNode(clusterId, groupId, serviceId uint) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and group_id = ? and type = ? and status = ? and service_id = ?", clusterId, groupId, def.TASK_TYPE_START, def.TASK_SUCCESS, serviceId)

	var res []*Task
	db = db.Find(&res)
	log.Info("clusterId", clusterId, " groupId:", groupId, " serviceId", serviceId, " response:", res)
	return res, db.Error
}

func Count(clusterId uint, updateTime int64) int64 {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0
	}
	var count int64
	db.Where("cluster_id = ? and update_time >= ?", clusterId, updateTime).Count(&count)
	return count
}

func DeleteByCluster(clusterId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ?", clusterId).Delete(&Task{})

	return db.Error
}

func DeleteByMachine(machineId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("machine_id = ?", machineId).Delete(&Task{})

	return db.Error
}

func DeleteByGroup(clusterId, groupId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).Delete(&Task{})

	return db.Error
}

func DeleteLittleNode(clusterId, groupId, nodeId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and group_id = ? and node_id < ?", clusterId, groupId, nodeId).Delete(&Task{})

	return db.Error
}

func GetListByClusterId(clusterId uint, updateTime int64, limit int, offset int) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and update_time >= ?", clusterId, updateTime)
	db = db.Order("update_time desc")
	db = db.Limit(limit).Offset(offset)

	res := []*Task{}
	db = db.Find(&res)
	for _, v := range res {
		json.Unmarshal([]byte(v.Extra), &v.TaskExt)
	}
	return res, db.Error
}

func GetListByGroup(clusterId, groupId uint) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId)

	res := []*Task{}
	db = db.Find(&res)
	for _, v := range res {
		json.Unmarshal([]byte(v.Extra), &v.TaskExt)
	}
	if db.Error != nil {
		log.Warnf("GetListByGroup DB clusterId:%d groupId:%d err:%v", clusterId, groupId, db.Error)
	}
	return res, db.Error
}

func GetClusterTaskInfo(clusterId uint) (string, string, error) {
	tasks, err := GetClusterTask(clusterId)
	if err != nil {
		return "", "", err
	}
	if len(tasks) == 0 {
		return "", "", errors.New("empty task")
	}
	return tasks[0].TaskExt.RegionName, tasks[0].TaskExt.DashboardName, nil
}

func GetClusterTask(clusterId uint) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and status = ? and type in (?, ?)", clusterId, def.TASK_SUCCESS, def.TASK_TYPE_START, def.TASK_TYPE_ADD).Limit(2)

	var res []*Task
	db = db.Find(&res)
	for _, v := range res {
		e := jsoniter.Unmarshal([]byte(v.Extra), &v.TaskExt)
		if e != nil {
			log.Warnf("unmarshal failed, err:%v", e)
		}
	}
	return res, db.Error
}

func GetInitTask(clusterId, groupId, machineId, nodeId uint) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and group_id = ? and machine_id = ? and node_id = ? and status = ? and type in (?, ?)", clusterId, groupId, machineId,
		nodeId, def.TASK_SUCCESS, def.TASK_TYPE_START, def.TASK_TYPE_ADD)

	var res []*Task
	db = db.Find(&res)
	for _, v := range res {
		e := jsoniter.Unmarshal([]byte(v.Extra), &v.TaskExt)
		if e != nil {
			log.Warnf("unmarshal failed, err:%v", e)
		}
	}
	return res, db.Error
}

func GetListByStatus(clusterId uint, status string) ([]*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if clusterId != 0 {
		db = db.Where("cluster_id = ? and status = ?", clusterId, status)
	} else {
		db = db.Where("status = ?", status)
	}

	res := []*Task{}
	db = db.Find(&res)
	for _, v := range res {
		json.Unmarshal([]byte(v.Extra), &v.TaskExt)
	}
	return res, db.Error
}
func GetInfo(taskId uint) (*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var task Task
	db = db.First(&task, taskId)
	json.Unmarshal([]byte(task.Extra), &task.TaskExt)
	return &task, db.Error
}

func CreateTask(task *Task) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	task.CreateTime = time.Now().Unix()
	task.UpdateTime = time.Now().Unix()
	s, err := json.Marshal(task.TaskExt)
	if err != nil {
		return err
	}

	task.Extra = string(s)
	return db.Create(task).Error
}

func Create(taskType, status string, regionId, machineId, serviceId uint, extra string) (*Task, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	task := &Task{
		Type:      taskType,
		Status:    status,
		RegionId:  regionId,
		MachineId: machineId,
		ServiceId: serviceId,
		// CosFileId:  cosFileId,
		Extra:      extra,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db = db.Create(task)
	return task, db.Error
}

func UpdateTask(taskId uint, task *Task) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	s, e := json.Marshal(task.TaskExt)
	if e != nil {
		return e
	}
	task.Extra = string(s)
	task.UpdateTime = time.Now().Unix()
	err = db.Where("id = ?", taskId).Updates(task).Error
	if err != nil {
		log.Warnf("update task %d err %v", taskId, err)
	}
	return err
}

func Update(taskId uint, cb func(task *Task) bool) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	task, e := GetInfo(taskId)
	if e != nil {
		return e
	}

	if db = db.Where("id = ?", taskId); cb(task) {
		log.Infof("taskext %#v", task.TaskExt)
		s, e := json.Marshal(task.TaskExt)
		if e != nil {
			return e
		}

		task.Extra = string(s)
		task.UpdateTime = time.Now().Unix()

		db = db.Updates(task)
	}
	return db.Error
}

func AddNotCheckType(t string) bool {
	return t == def.TASK_TYPE_CGROUP || t == def.TASK_TYPE_LINK
}

func CheckNotifyType(t string) bool {
	return t == def.TASK_TYPE_PREPARESTART || t == def.TASK_TYPE_PREPAREADD || t == def.TASK_TYPE_ADD || t == def.TASK_TYPE_START
}
