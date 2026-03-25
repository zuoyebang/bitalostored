package helper

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"machine-exporter/collector"
	"machine-exporter/model"
	"net/http"
	"strconv"
	"time"

	"gorm.io/gorm"
)

func GetAllNode() []*model.TblMachine {
	res := make([]*model.TblMachine, 0)
	err := MysqlPass.Model(&model.TblMachine{}).Where("status = ?", "online").Preload("TblNode", func(db *gorm.DB) *gorm.DB {
		return db.Where("status = ? and is_witness = 0", "online").Preload("TblCluster")
	}).Find(&res).Error
	if err != nil {
		panic(err)
	}
	return res
}

func GetMachineCpuLimit(machineId int) ([]*model.TblNode, error) {
	var res []*model.TblNode
	err := MysqlPass.Model(&model.TblNode{}).Select("cpu_throttled_nr, service_port").Where("machine_id = ?", machineId).Find(&res).Error
	if err != nil {
		return res, err
	}
	return res, nil
}

func GetNodeCpuLimit(machineId int, servicePort string) (*model.TblNode, error) {
	var res *model.TblNode
	err := MysqlPass.Model(&model.TblNode{}).Select("cpu_throttled_nr").Where("machine_id = ? and service_port = ?", machineId, servicePort).Find(&res).Error
	if err != nil {
		return res, err
	}
	return res, nil
}

func Format(machine []*model.TblMachine) ([]collector.Instance, []collector.Instance) {
	resProxy := make([]collector.Instance, 0)
	resBitalos := make([]collector.Instance, 0)
	for _, v := range machine {
		for _, v1 := range v.TblNode {
			if v1.ServiceID == 2 {
				resProxy = append(resProxy, collector.Instance{
					IP:         v.IP,
					Port:       strconv.Itoa(v1.ServicePort),
					AdminPort:  strconv.Itoa(v1.ClusterPort),
					Name:       v1.TblCluster.Name,
					Idc:        v.Idc,
					MachineId:  v.ID,
					HttpClient: GetHttpPool(),
					StoredPool: GetStoredPool(v.IP, v1.ServicePort),
				})
			}
			if v1.ServiceID == 1 || v1.ServiceID == 6 {
				resBitalos = append(resBitalos, collector.Instance{
					IP:         v.IP,
					Port:       strconv.Itoa(v1.ServicePort),
					AdminPort:  strconv.Itoa(v1.ClusterPort),
					Name:       v1.TblCluster.Name,
					Idc:        v.Idc,
					Group:      strconv.Itoa(v1.GroupID),
					MachineId:  v.ID,
					StoredPool: GetStoredPool(v.IP, v1.ServicePort),
				})
			}
		}
	}
	return resProxy, resBitalos
}

func GetStoredPool(ip string, port int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,
		MaxActive:   5,
		IdleTimeout: 1800 * time.Second,
		Wait:        true,
		Dial: func() (conn redis.Conn, e error) {
			conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", ip, port),
				redis.DialConnectTimeout(time.Second),
				redis.DialReadTimeout(time.Second),
				redis.DialWriteTimeout(time.Second),
			)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < 120*time.Second {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func GetHttpPool() *http.Client {
	return &http.Client{
		Timeout: 1 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        5,
			MaxIdleConnsPerHost: 5,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}
