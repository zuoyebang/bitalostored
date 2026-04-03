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

package tbl_cluster

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sync"
	"time"

	"gorm.io/gorm"
)

const TableName = "tblCluster"

type Cluster struct {
	Id     uint   `gorm:"column:id;primary_key" json:"clusterId"`
	Name   string `gorm:"column:name" json:"clusterName"`
	Status string `gorm:"column:status" json:"clusterStatus"`

	RegionId  uint `gorm:"column:region_id" json:"regionId"`
	ServiceId uint `gorm:"column:service_id" json:"serviceId"`
	StoredId  uint `gorm:"column:stored_id" json:"storedId"`

	ConfigPackId uint   `gorm:"column:config_pack_id" json:"configPackId"`
	StoredAuth   string `gorm:"column:auth" json:"storedAuth"`
	DeraftToken  string `gorm:"column:deraft_token" json:"deraftToken"`
	Department   string `gorm:"column:department" json:"department"`
	IsStored1    bool   `gorm:"column:is_stored1" json:"isStored1"`
	ClusterGroup string `gorm:"column:cluster_group" json:"clusterGroup"`

	MonitorLink string `gorm:"column:monitor" json:"jumpAddress"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetList(regionId, serviceId uint, status string) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = genQueryDB(regionId, serviceId, status, db)

	res := []*Cluster{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetListByIds(clusterIds []uint) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var clusters []*Cluster
	db = db.Where(clusterIds).Find(&clusters)
	return clusters, db.Error
}

func GetInfoByStoredId(storedId, serviceId uint) (*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("stored_id = ? and service_id = ? and status = ?", storedId, serviceId, def.CLUSTER_STATUS_ONLINE)

	res := &Cluster{}
	db.First(res)
	return res, db.Error
}

func GetListByDepartment(serviceId uint, department string, page int, num int) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if department == "" {
		if serviceId == def.SERVICE_ID_MATRIX {
			db = db.Where("service_id in (?,?) and status = ?", serviceId, def.SERVICE_ID_BITALOS, def.CLUSTER_STATUS_ONLINE)
		} else {
			db = db.Where("service_id = ? and status = ?", serviceId, def.CLUSTER_STATUS_ONLINE)
		}
	} else {
		if serviceId == def.SERVICE_ID_MATRIX {
			db = db.Where("service_id in (?,?) and department = ?  and status = ?", serviceId, def.SERVICE_ID_BITALOS, department, def.CLUSTER_STATUS_ONLINE)
		} else {
			db = db.Where("service_id = ? and department = ?  and status = ?", serviceId, department, def.CLUSTER_STATUS_ONLINE)
		}
	}
	var res []*Cluster
	db = db.Order("id").Offset((page - 1) * num).Limit(num)
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetCountByDepartment(serviceId uint, department string) (int64, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	if department == "" {
		if serviceId == def.SERVICE_ID_MATRIX {
			db = db.Where("service_id in (?,?) and status = ?", serviceId, def.SERVICE_ID_BITALOS, def.CLUSTER_STATUS_ONLINE)
		} else {
			db = db.Where("service_id = ? and status = ?", serviceId, def.CLUSTER_STATUS_ONLINE)
		}
	} else {
		if serviceId == def.SERVICE_ID_MATRIX {
			db = db.Where("service_id in (?,?) and department = ?  and status = ?", serviceId, def.SERVICE_ID_BITALOS, department, def.CLUSTER_STATUS_ONLINE)
		} else {
			db = db.Where("service_id = ? and department = ?  and status = ?", serviceId, department, def.CLUSTER_STATUS_ONLINE)
		}
	}
	var num int64
	db.Count(&num)
	return num, db.Error
}

func GetListByRegion(regionId, serviceId uint) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("region_id = ? and service_id = ? and status = ?", regionId, serviceId, def.CLUSTER_STATUS_ONLINE)
	var res []*Cluster
	db = db.Find(&res)
	return res, db.Error
}

func GetListByNs(name string, serviceId uint) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if serviceId == def.SERVICE_ID_MATRIX || serviceId == def.SERVICE_ID_BITALOS {
		db = db.Where("name = ? and service_id in (?,?) and status = ?", name, def.SERVICE_ID_MATRIX, def.SERVICE_ID_BITALOS, def.CLUSTER_STATUS_ONLINE)
	} else {
		db = db.Where("name = ? and service_id = ? and status = ?", name, serviceId, def.CLUSTER_STATUS_ONLINE)
	}
	var res []*Cluster
	db = db.Find(&res)
	return res, db.Error
}

func GetListByName(name string) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("name = ? and status = ?", name, def.CLUSTER_STATUS_ONLINE)
	var res []*Cluster
	db = db.Find(&res)
	if db.Error != nil {
		log.Errorf("tbl_cluster GetListByName %s failed: [%v]", name, db.Error)
	}
	return res, db.Error
}

func GetClusterNames() ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Select("id, name").Where("status = ?", def.CLUSTER_STATUS_ONLINE)
	var res []*Cluster
	db = db.Find(&res)
	return res, db.Error
}

func GetListByServiceId(serviceId uint) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Where("service_id = ? and status = ?", serviceId, def.CLUSTER_STATUS_ONLINE)
	var res []*Cluster
	db = db.Find(&res)
	return res, db.Error
}

func GetDepartmentList(serviceId uint) ([]string, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if serviceId == def.SERVICE_ID_MATRIX {
		db = db.Where("service_id in (?,?) and status = ?", def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX, def.CLUSTER_STATUS_ONLINE)
	} else {
		db = db.Where("service_id = ? and status = ?", serviceId, def.CLUSTER_STATUS_ONLINE)
	}
	var res []*Cluster
	db = db.Find(&res)
	var ds []string
	ex := make(map[string]bool, 0)
	for _, r := range res {
		if _, ok := ex[r.Department]; !ok {
			ds = append(ds, r.Department)
		}
		ex[r.Department] = true
	}
	return ds, db.Error
}

func GetClusterListByDepartment(department string) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	res := []*Cluster{}
	if department == "" {
		db.Find(&res)
	} else {
		db.Where("department = ? and status = ?", department, def.CLUSTER_STATUS_ONLINE).Find(&res)
	}
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func Delete(clusterId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("id = ?", clusterId).Delete(&Cluster{})

	return db.Error
}

func GetClusterServerList() ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Where("service_id IN (?)", []uint{def.SERVICE_ID_MATRIX, def.SERVICE_ID_BITALOS})
	db = db.Where("status = ?", def.CLUSTER_STATUS_ONLINE)

	var res []*Cluster
	db.Order("stored_id desc").Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetClusterServerProxyList() ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Where("service_id IN (?)", []uint{def.SERVICE_ID_MATRIX, def.SERVICE_ID_BITALOS, def.SERVICE_ID_PROXY})
	db = db.Where("status = ?", def.CLUSTER_STATUS_ONLINE)

	var res []*Cluster
	db.Order("stored_id desc").Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetNamesByServiceId(serviceId uint) ([]string, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	if serviceId == def.SERVICE_ID_MATRIX || serviceId == def.SERVICE_ID_BITALOS {
		db = db.Distinct("name").Where("service_id in (?,?) and status = ?", def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX, def.CLUSTER_STATUS_ONLINE)
	} else {
		db = db.Distinct("name").Where("service_id = ? and status = ?", serviceId, def.CLUSTER_STATUS_ONLINE)
	}
	var res []string
	db = db.Find(&res)
	return res, db.Error
}

func SameStoredCluster(storedId uint) ([]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	res := []*Cluster{}
	db = db.Where("stored_id = ? and status = ?", storedId, def.CLUSTER_STATUS_ONLINE).Find(&res)
	return res, db.Error
}

func UpdateClusterStoredId(oldStoredId, newStoredId, skipServiceId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db.Model(&Cluster{}).Where("stored_id = ? and service_id != ", oldStoredId, skipServiceId).UpdateColumn("stored_id", newStoredId)
	return db.Error
}

func Count(regionId, serviceId uint, status string) int64 {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0
	}
	var count int64
	genQueryDB(regionId, serviceId, status, db).Count(&count)
	return count
}
func GetInfo(ID uint) (*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("id = ?", ID)

	res := &Cluster{}
	db.First(res)
	return res, db.Error
}

func GetInfoByName(name string, serviceId uint) (*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("name = ? and service_id = ? and status = 'online'", name, serviceId)

	res := &Cluster{}
	db.First(res)
	return res, db.Error
}

func GetServerListByNames(names []string) (map[string]*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	serverService := []int{def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX}
	db = db.Where("name in (?) and status = 'online' and service_id in (?)", names, serverService)

	var res []*Cluster
	db.Find(&res)
	ret := make(map[string]*Cluster, len(res))
	for _, r := range res {
		if r.Name == "our-search-page" {
			r.Name = "ocr-search-page"
		}
		if r.Name == "our-search-inv" {
			r.Name = "ocr-search-inv"
		}
		ret[r.Name] = r
	}
	return ret, db.Error
}

func genQueryDB(regionId, serviceId uint, status string, db *gorm.DB) *gorm.DB {
	if status != "" {
		if regionId != 0 {
			if serviceId != 0 {
				db = db.Where("region_id = ? and service_id = ? and status in (?)", regionId, serviceId, []string{status})
			} else {
				db = db.Where("region_id = ? and status in (?)", regionId, []string{status})
			}
		} else {
			if serviceId != 0 {
				db = db.Where("service_id = ? and status in (?)", serviceId, []string{status})
			} else {
				db = db.Where("status in (?)", []string{status})
			}
		}
	} else {
		if regionId != 0 {
			if serviceId != 0 {
				db = db.Where("region_id = ? and service_id = ?", regionId, serviceId)
			} else {
				db = db.Where("region_id = ? ", regionId)
			}
		} else {
			if serviceId != 0 {
				db = db.Where("service_id = ?", serviceId)
			}
		}
	}
	return db
}

func Create(name string, regionId, serviceId, storedId, configPackId uint, department string) (*Cluster, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	createLock.Lock()
	defer createLock.Unlock()

	if storedId == 1 && serviceId == 3 {
		maxStoredId, err := getMaxStoredId(db)
		if err != nil {
			return nil, err
		}
		if maxStoredId >= storedId {
			storedId = maxStoredId + 1
		}
	}

	res := &Cluster{
		Name:         name,
		Status:       def.CLUSTER_STATUS_ONLINE,
		RegionId:     regionId,
		ServiceId:    serviceId,
		StoredId:     storedId,
		ConfigPackId: configPackId,
		DeraftToken:  math2.GetMd5(name),
		Department:   department,
		CreateTime:   time.Now().Unix(),
		UpdateTime:   time.Now().Unix(),
	}
	db.Create(res)
	return res, db.Error
}
func Update(id uint, cluster Cluster) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	cluster.UpdateTime = time.Now().Unix()
	db.First(&Cluster{}, id).UpdateColumns(cluster)
	return db.Error
}

var createLock sync.Mutex

func getMaxStoredId(db *gorm.DB) (uint, error) {
	var max maxStruct
	d := db.Raw("SELECT MAX( tblCluster.stored_id ) AS max FROM tblCluster").Scan(&max)
	return max.Max, d.Error
}

type maxStruct struct {
	Max uint `json:"max"`
}
