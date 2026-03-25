package collect

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/connector"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"strings"

	"gorm.io/gorm"
)

const TableServerActionLog = "tbl_server_action_log"

type ServerActionLogModel struct {
	Id              int64   `gorm:"column:id"`
	Ip              string  `gorm:"column:ip"`
	Port            int64   `gorm:"column:port"`
	ClusterName     string  `gorm:"column:cluster_name"`
	ActionType      uint8   `gorm:"actionType"`
	DbType          string  `gorm:"dbType"`
	ActionSize      string  `gorm:"actionSize"`
	KeyNums         int64   `gorm:"keyNums"`
	ActionStartTime int64   `gorm:"column:action_start_time"`
	ActionEndTime   int64   `gorm:"column:action_end_time"`
	Duration        float64 `gorm:"duration"`
	Job             string  `gorm:"job"`
	RawContent      string  `gorm:"raw_content"`
	CreateTime      int64   `gorm:"column:create_time"`
	UpdateTime      int64   `gorm:"column:update_time"`
}

func (model *ServerActionLogModel) TableName() string {
	return TableServerActionLog
}

type ServerActionLogService struct {
	Model *gorm.DB
}

func NewServerActionLogService() *ServerActionLogService {
	m := new(ServerActionLogService)
	m.Model = connector.BsClient.Model(&ServerActionLogModel{})
	return m
}

func (s *ServerActionLogService) InsertLog(data *ServerActionLogModel) error {
	sql := fmt.Sprintf("insert into %s (cluster_name,ip,raw_content,port,action_type,action_size,key_nums,action_start_time,action_end_time,duration,job,create_time,update_time, db_type) values('%s','%s','%s',%d,%d,'%s',%d,%d,%d,%f,'%s',%d, %d,'%s')",
		TableServerActionLog,
		data.ClusterName,
		data.Ip,
		data.RawContent,
		data.Port,
		data.ActionType,
		data.ActionSize,
		data.KeyNums,
		data.ActionStartTime,
		data.ActionEndTime,
		data.Duration,
		data.Job,
		data.CreateTime,
		data.UpdateTime,
		data.DbType,
	)
	s.Model = s.Model.Exec(sql)
	return s.Model.Error
}

func (s *ServerActionLogService) MultiInsertLog(datas []*ServerActionLogModel) error {
	sqlHeader := "insert into tbl_server_action_log (cluster_name,ip,raw_content,port,action_type,action_size,key_nums,action_start_time,action_end_time,duration,job,create_time,update_time, db_type) values"
	var sqlContent string
	for _, m := range datas {
		sqlContent += fmt.Sprintf("('%s','%s','%s',%d,%d,'%s',%d,%d,%d,%f,'%s',%d, %d,'%s'),",
			m.ClusterName, m.Ip, m.RawContent, m.Port, m.ActionType, m.ActionSize, m.KeyNums, m.ActionStartTime,
			m.ActionEndTime, m.Duration, m.Job, m.CreateTime, m.UpdateTime, m.DbType)
	}
	sqlContent = strings.TrimSuffix(sqlContent, ",")
	err := s.Model.Exec(sqlHeader + sqlContent).Error
	if err != nil {
		logs.Errorf("batch insert %s error:%v", TableServerActionLog, err)
		return err
	}
	return nil
}

func (s *ServerActionLogService) UpdateLog(data *ServerActionLogModel) error {
	err := s.Model.Where("id = ?", data.Id).Updates(data).Error
	if err != nil {
		logs.Errorf("update server action log failed, err: %v, data: %s", err, data.RawContent)
		return err
	}
	return nil
}
