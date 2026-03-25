package collect

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/connector"

	"gorm.io/gorm"
)

const TableSlowLog = "tbl_slow_log"

type SlowLogModel struct {
	Id          int64  `gorm:"column:id"`
	Service     string `gorm:"column:service"`
	ClusterName string `gorm:"column:cluster_name"`
	NodeIp      string `gorm:"column:node_ip"`
	IDC         string `gorm:"column:idc"`
	NodePort    int64  `gorm:"column:node_port"`
	Duration    int64  `gorm:"column:duration"`
	Key         string `gorm:"column:key"`
	Command     string `gorm:"column:command"`
	Params      string `gorm:"column:params"`
	LogTime     int64  `gorm:"column:log_time"`
	CreateTime  int64  `gorm:"column:create_time"`
	UpdateTime  int64  `gorm:"column:update_time"`
}

func (model *SlowLogModel) TableName() string {
	return TableSlowLog
}

type SlowLogService struct {
	Model *gorm.DB
}

func NewSlowLogService() *SlowLogService {
	m := new(SlowLogService)
	m.Model = connector.BsClient.Model(&SlowLogModel{})
	return m
}

func (s *SlowLogService) Insert(data SlowLogModel) error {
	sql := fmt.Sprintf("insert into %s (cluster_name,service,node_ip,idc,node_port,duration,`key`,command,params,log_time,create_time,update_time) values('%s', '%s', '%s', '%s', %d, %d,  '%s', '%s', '%s',%d, %d, %d)",
		TableSlowLog,
		data.ClusterName,
		data.Service,
		data.NodeIp,
		data.IDC,
		data.NodePort,
		data.Duration,
		data.Key,
		data.Command,
		data.Params,
		data.LogTime,
		data.CreateTime,
		data.UpdateTime,
	)

	s.Model = s.Model.Exec(sql)
	return s.Model.Error
}
