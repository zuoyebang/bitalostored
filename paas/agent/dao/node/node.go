package tbl_node

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/connector"
	log "github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"strconv"
)

const TableName = "tblNode"

type Node struct {
	ClusterId uint   `gorm:"column:cluster_id" json:"clusterId"`
	GroupId   uint   `gorm:"column:group_id" json:"groupId"`
	NodeId    uint   `gorm:"column:node_id" json:"nodeId"`
	Status    string `gorm:"column:status" json:"nodeStatus"`

	RegionId  uint `gorm:"column:region_id" json:"regionId"`
	MachineId uint `gorm:"column:machine_id" json:"machineId"`
	ServiceId uint `gorm:"column:service_id" json:"serviceId"`

	IsWitness      bool   `gorm:"column:is_witness" json:"isWitness"`
	CosFileId      uint   `gorm:"column:cos_file_id" json:"cosFileId"`
	CosFileVersion string `gorm:"column:cos_file_version" json:"version"`
	ConfigContent  string `gorm:"column:config_content" json:"configContent"`
	CpuThrottledNr int    `gorm:"column:cpu_throttled_nr" json:"cpuThrottledNr"`

	ServicePort uint `gorm:"column:service_port" json:"servicePort"`
	ClusterPort uint `gorm:"column:cluster_port" json:"clusterPort"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetInstance(machineId uint, servicePort int64) ([]*Node, error) {
	db, err := connector.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Node
	db = db.Model(&Node{}).Where("machine_id = (?) and service_port = ?", machineId, servicePort).Find(&res)
	return res, db.Error
}

func UpdateCpuThrottled(machineId uint, portThrottled map[string]string) error {
	if len(portThrottled) <= 0 {
		return nil
	}
	db, err := connector.GetDB(TableName)
	if err != nil {
		return err
	}
	for p, t := range portThrottled {
		port, _ := strconv.Atoi(p)
		throttled, _ := strconv.Atoi(t)
		res := db.Exec("update tblNode set cpu_throttled_nr = ? where machine_id = ? and service_port = ?", throttled, machineId, port)
		//res := db.Model(&Node{}).Where("machine_id = ? and service_port = ?", machineId, port).Update("cpu_throttled_nr", throttled)
		//log.Infof("port:%d, throttled:%d, mid:%d affect:%d", port, throttled, machineId, res.RowsAffected)
		if res.Error != nil {
			log.Errorf("update tblNode throttled faild, mid=%d, port=%d, throttled=%d", machineId, port, throttled)
		}
	}
	return nil
}
