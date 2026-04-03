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

package redis_op

import (
	"github.com/zuoyebang/bitalostored/paas/dao/redis_cli"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
	"time"
)

func GetInfo(address string, clusterId uint, groupId uint, clusterName string) (map[string]string, error) {
	cli, err := redis_cli.NewClient(address, config.GetAuth(clusterId, clusterName), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		return nil, err
	}
	infos, err := cli.MergeInfoV67(uint64(groupId))
	if err != nil {
		log.Errorf("could not get redis info.err:%+v", err)
		return nil, err
	}
	return infos, nil
}

func GetNodeRole(address string, clusterId uint, groupId uint, clusterName string) (string, error) {
	cli, err := redis_cli.NewClient(address, config.GetAuth(clusterId, clusterName), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		return "", err
	}
	defer cli.Close()
	infos, err := cli.MergeInfoV67(uint64(groupId))
	if err != nil {
		log.Errorf("could not get redis info.err:%+v", err)
		return "", err
	}
	role, ok := infos["role"]
	if !ok {
		log.Warnf("check master. redis info missing role message.redis addr:%s groupId:%d  info:%+v", address, groupId, infos)
		return "", errors.New("no role")
	}
	if infos["start_model"] == def.NODE_ROLE_OBSERVER && role == def.NODE_ROLE_SLAVE {
		role = def.NODE_ROLE_OBSERVER
	}
	return role, nil
}

func MayBeNodeMaster(address string, clusterId uint, groupId uint, clusterName string) bool {
	role, err := GetNodeRole(address, clusterId, groupId, clusterName)
	if err != nil {
		return true
	}
	if role == def.NODE_ROLE_MASTER {
		return true
	}
	return false
}

func IsSingle(address string, clusterId, groupId uint, clusterName string) (bool, error) {
	role, err := GetNodeRole(address, clusterId, groupId, clusterName)
	if err != nil {
		return false, err
	}
	if role == def.NODE_ROLE_SINGLE {
		return true, nil
	}
	return false, nil
}

func GetRaftRole(infos map[string]string) string {
	if infos == nil {
		return ""
	}
	role, ok := infos["role"]
	if !ok {
		log.Warnf("get raft role. redis info missing role message.redis info:%+v", infos)
		return ""
	}
	if infos["start_model"] == def.NODE_ROLE_OBSERVER && role == def.NODE_ROLE_SLAVE {
		role = def.NODE_ROLE_OBSERVER
	}
	return role
}

func UpgradeRedis(address, clusterName string, clusterId uint) error {
	before, err := getStartTime(address, clusterName, clusterId)
	if err != nil {
		return err
	}
	log.Infof("address:%v time:%v before shutdown.", address, before)
	err = shutdown(address, clusterName, clusterId)
	if err != nil {
		return err
	}
	time.Sleep(10 * time.Second)
	var after string
	for i := 0; i < 18; i++ {
		after, err = getStartTime(address, clusterName, clusterId)
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	log.Infof("address:%v time:%v before shutdown.", address, after)
	if err != nil {
		return err
	}
	if strings.TrimSpace(before) == strings.TrimSpace(after) {
		log.Warnf("shutdown function incorrect")
		return errors.New("shutdown function incorrect")
	}
	return nil
}

func getStartTime(address, clusterName string, clusterId uint) (string, error) {
	cli, err := redis_cli.NewClient(address, config.GetAuth(clusterId, clusterName), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		return "", err
	}
	infos, err := cli.Info()
	if err != nil {
		log.Errorf("could not get redis info.err:%+v", err)
		return "", err
	}
	startTime, ok := infos["start_time"]
	if !ok {
		log.Warnf("redis info missing start_time message.redis info:%+v", infos)
		return "", errors.New("redis info missing start_time message")
	}
	return startTime, nil
}

func shutdown(address, clusterName string, clusterId uint) error {
	cli, err := redis_cli.NewClient(address, config.GetAuth(clusterId, clusterName), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		return err
	}
	err = cli.Shutdown()
	if err != nil && err.Error() != "EOF" {
		log.Errorf("could not shutdown redis.err:%+v", err)
		return err
	}
	return nil
}
