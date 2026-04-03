package collect

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/connector"

	"gorm.io/gorm"
)

const TableExceptionLog = "tbl_exception_log"

type ExceptionLogModel struct {
	Id            int64  `gorm:"column:id"`
	ClusterName   string `gorm:"column:cluster_name"`
	NodeIp        string `gorm:"column:node_ip"`
	DstIp         string `gorm:"column:dst_ip"`
	IDC           string `gorm:"column:idc"`
	GroupId       int64  `gorm:"column:group_id"`
	SlotId        int64  `gorm:"column:slot_id"`
	LogType       uint8  `gorm:"column:log_type"`
	LogLevel      uint8  `gorm:"column:log_level"`
	NodePort      int64  `gorm:"column:node_port"`
	DstPort       int64  `gorm:"column:dst_port"`
	Key           string `gorm:"column:key"`
	Command       string `gorm:"column:command"`
	ExceptionInfo string `gorm:"column:exception_info"`
	RawContent    string `gorm:"column:raw_content"`
	LogTime       int64  `gorm:"column:log_time"`
	CreateTime    int64  `gorm:"column:create_time"`
	UpdateTime    int64  `gorm:"column:update_time"`
}

func (model *ExceptionLogModel) TableName() string {
	return TableExceptionLog
}

type ExceptionLogService struct {
	Model *gorm.DB
}

func NewExceptionLogService() *ExceptionLogService {
	m := new(ExceptionLogService)
	m.Model = connector.BsClient.Model(&ExceptionLogModel{})
	return m
}

func (s *ExceptionLogService) Insert(data ExceptionLogModel) error {
	sql := fmt.Sprintf("insert into %s (cluster_name,node_ip,dst_ip,idc,log_type,log_level,node_port,dst_port,`key`,command,exception_info,raw_content,log_time,create_time,update_time,group_id,slot_id) values('%s','%s','%s','%s',%d,%d,%d,%d,'%s','%s', '%s', '%s', %d, %d, %d, %d, %d)",
		TableExceptionLog,
		data.ClusterName,
		data.NodeIp,
		data.DstIp,
		data.IDC,
		data.LogType,
		data.LogLevel,
		data.NodePort,
		data.DstPort,
		data.Key,
		data.Command,
		data.ExceptionInfo,
		data.RawContent,
		data.LogTime,
		data.CreateTime,
		data.UpdateTime,
		data.GroupId,
		data.SlotId,
	)

	s.Model = s.Model.Exec(sql)
	return s.Model.Error
}
