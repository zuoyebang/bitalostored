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

package tbl_config

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sync"
	"time"
)

var TableName = "tblConfig"
var createLock sync.Mutex

type TblConfig struct {
	ID       uint   `gorm:"column:id;primary_key" json:"id"`
	Name     string `gorm:"column:name" json:"name"`
	FileType string `gorm:"column:file_type" json:"fileType"`
	FileMode string `gorm:"column:file_mode" json:"fileMode"`
	IDC      string `gorm:"column:idc" json:"idc"`

	LastVersion    string `gorm:"column:last_version" json:"-"`
	ConfigPackName string `gorm:"column:config_pack_name" json:"configPackName"`
	ConfigPackId   uint   `gorm:"column:config_pack_id" json:"configPackId"`
	ServiceId      uint   `gorm:"column:service_id" json:"serviceId"`
	ClusterId      uint   `gorm:"column:cluster_id" json:"cluster_id"`
	NeedRender     bool   `gorm:"column:need_render" json:"needRender"`
	Content        string `gorm:"column:content" json:"content"`
	Comment        string `gorm:"column:comment" json:"comment"`

	ButtonName string `gorm:"-" json:"buttonName"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func Create(name, idc, fileType, fileMode, content, comment, configPackName, buttonName string, serviceId, configPackId uint, needRender bool) (*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	createLock.Lock()
	defer createLock.Unlock()

	res := &TblConfig{
		Name:           name,
		IDC:            idc,
		FileType:       fileType,
		FileMode:       fileMode,
		Content:        content,
		Comment:        comment,
		ConfigPackName: configPackName,
		ServiceId:      serviceId,
		ConfigPackId:   configPackId,
		NeedRender:     needRender,
		CreateTime:     time.Now().Unix(),
		UpdateTime:     time.Now().Unix(),
	}
	db.Create(res)
	return res, db.Error
}

func getMaxPackId(serviceId uint) (uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	var res TblConfig
	if serviceId == def.SERVICE_ID_BITALOS || serviceId == def.SERVICE_ID_MATRIX {
		db = db.Where("service_id in (?,?)", def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX).Order("config_pack_id desc").First(&res)
	} else {
		db = db.Where("service_id = ?", serviceId).Order("config_pack_id desc").First(&res)
	}
	return res.ConfigPackId, db.Error
}

func GetFirstConfigPack(serviceId uint, packName string) (*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var configs *TblConfig
	db.Where("service_id = ? and config_pack_name = ? and cluster_id = 0", serviceId, packName).First(&configs)
	return configs, db.Error
}

func ConfigPackList(serviceId uint) ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var configs []*TblConfig
	if serviceId == def.SERVICE_ID_MATRIX {
		db.Where("service_id in (?,?) and cluster_id = 0", serviceId, def.SERVICE_ID_BITALOS).Find(&configs)
	} else {
		db.Where("service_id = ? and cluster_id = 0", serviceId).Find(&configs)
	}
	return configs, db.Error
}

func UpdateClusterId(configPackId, clusterId, serviceId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	var configs []*TblConfig
	if def.IsServer(serviceId) {
		db.Where("config_pack_id = ? and service_id = ?", configPackId, def.SERVICE_ID_BITALOS).Find(&configs)
	} else {
		db.Where("config_pack_id = ? and service_id = ?", configPackId, serviceId).Find(&configs)
	}
	for _, conf := range configs {
		db, err = dao.GetDB(TableName)
		if err != nil {
			continue
		}
		db.Where("id = ?", conf.ID).Updates(&TblConfig{ClusterId: clusterId})
	}
	if db.Error != nil {
		log.Warnf("update tblConfig clusterId failed.configPackId:%d clusterId:%d serviceId:%d err:%v", configPackId, clusterId, serviceId, db.Error)
	}
	return db.Error
}

func GetList(limit, offset int) ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var configs []*TblConfig
	db.Limit(limit).Offset(offset).Find(&configs)
	for i, c := range configs {
		configs[i].ButtonName = c.Name
		if c.IDC != "" {
			configs[i].ButtonName = configs[i].ButtonName + "_" + c.IDC
		}
	}
	return configs, db.Error
}

func GetListByPack(configPackId, serviceId uint) ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var configs []*TblConfig
	if serviceId == def.SERVICE_ID_MATRIX {
		db.Where("config_pack_id = ? and service_id in(?,?)", configPackId, def.SERVICE_ID_MATRIX, def.SERVICE_ID_BITALOS).Find(&configs)
	} else {
		db.Where("config_pack_id = ? and service_id = ?", configPackId, serviceId).Find(&configs)
	}
	for i, c := range configs {
		configs[i].ButtonName = c.Name
		if c.IDC != "" {
			configs[i].ButtonName = configs[i].ButtonName + "_" + c.IDC
		}
		if c.ServiceId == def.SERVICE_ID_BITALOS {
			configs[i].ButtonName = "bitalos-" + configs[i].ButtonName
		}
		if c.ServiceId == def.SERVICE_ID_MATRIX {
			configs[i].ButtonName = "matrix-" + configs[i].ButtonName
		}
	}

	sortedConfigs := sortConfigList(configs, int(serviceId))
	return sortedConfigs, db.Error
}

func GetClusterConfig(serviceId uint, configName, clusterName string) ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var configs []*TblConfig
	db.Where("name = ? and service_id = ? and config_pack_name = ?", configName, serviceId, clusterName).Find(&configs)
	return configs, db.Error
}

func GetServerConfigList() ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var configs []*TblConfig
	db.Where("service_id = ? and name = ?", def.SERVICE_ID_BITALOS, "config/config.toml").Find(&configs)
	return configs, db.Error
}

func GetListByClusterName(clusterName string) ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var configs []*TblConfig
	db.Where("config_pack_name = ?", clusterName).Find(&configs)
	for i, c := range configs {
		configs[i].ButtonName = c.Name
		if c.IDC != "" {
			configs[i].ButtonName = configs[i].ButtonName + "_" + c.IDC
		}
	}
	return configs, db.Error
}

func GetNoClusterOwnerConfigs(excludePackId []uint, serviceId uint) ([]*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var configs []*TblConfig
	db.Where("config_pack_id not in (?) and service_id = ?", excludePackId, serviceId).Find(&configs)
	for i, c := range configs {
		configs[i].ButtonName = c.Name
		if c.IDC != "" {
			configs[i].ButtonName = configs[i].ButtonName + "_" + c.IDC
		}
	}
	return configs, db.Error
}

func CreateConfigs(serviceId uint, cs []*TblConfig) (uint, error) {
	createLock.Lock()
	defer createLock.Unlock()
	id, err := getMaxPackId(serviceId)
	if err != nil {
		return 0, err
	}
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	for _, c := range cs {
		res := &TblConfig{
			Name:           c.Name,
			IDC:            c.IDC,
			FileType:       c.FileType,
			FileMode:       c.FileMode,
			Content:        c.Content,
			Comment:        c.Comment,
			ConfigPackName: c.ConfigPackName,
			ServiceId:      c.ServiceId,
			ConfigPackId:   id + 1,
			NeedRender:     c.NeedRender,
			CreateTime:     time.Now().Unix(),
			UpdateTime:     time.Now().Unix(),
		}
		if err = db.Create(res).Error; err != nil {
			return 0, err
		}
	}
	return id + 1, nil
}

func getConfig(id uint) (*TblConfig, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res TblConfig
	db.First(&res, id)
	return &res, db.Error
}

func DeleteConfigs(clusterId, configPackId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and config_pack_id = ?", clusterId, configPackId).Delete(&TblConfig{})
	return db.Error
}

func UpdateConfigs(cs []*TblConfig) (uint, error) {
	if len(cs) == 0 {
		return 0, nil
	}

	var configPackId uint
	for _, c := range cs {
		db, err := dao.GetDB(TableName)
		if err != nil {
			return 0, err
		}
		var res TblConfig
		db.First(&res, c.ID)
		if db.Error != nil {
			log.Errorf("select id=%d, err=%v", c.ID, db.Error)
			return 0, db.Error
		}
		configPackId = c.ConfigPackId
		if res.Content == c.Content {
			continue
		}
		c.UpdateTime = time.Now().Unix()
		db.Where("id = ?", c.ID).Updates(TblConfig{
			Content:     c.Content,
			LastVersion: res.Content,
			Comment:     c.Comment,
		})
		if db.Error != nil {
			log.Errorf("update id=%d", c.ID)
			return 0, db.Error
		}
	}
	return configPackId, nil
}

func sortConfigList(srcList []*TblConfig, serviceId int) (dstConfig []*TblConfig) {
	if serviceId != def.SERVICE_ID_PROXY && serviceId != def.SERVICE_ID_DASHBOARD {
		return srcList
	}

	var configHead []string
	configPos := make(map[string]int)
	dstConfig = make([]*TblConfig, len(srcList))
	nextIdx := 0
	if serviceId == def.SERVICE_ID_DASHBOARD {
		configHead = []string{"config/dashboard.toml", "run.sh"}
	}

	if serviceId == def.SERVICE_ID_PROXY {
		configHead = []string{"config/proxy-txcloud.toml"}
	}

	for _, c := range configHead {
		configPos[c] = nextIdx
		nextIdx++
	}
	for _, c := range srcList {
		if idx, ok := configPos[c.Name]; ok {
			dstConfig[idx] = c
			continue
		} else {
			configPos[c.Name] = nextIdx
			dstConfig[nextIdx] = c
			nextIdx++
		}
	}
	return dstConfig
}
