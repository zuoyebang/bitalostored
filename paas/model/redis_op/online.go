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
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
	"time"
)

func IsOnline(address string, clusterId uint) bool {
	_, err := redis_cli.NewClient(address, config.GetAuth(clusterId, ""), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		return false
	}
	return true
}

func CheckOnlineRepeatly(addr string, clusterId uint) bool {
	for i := 0; i < 3; i++ {
		if IsOnline(addr, clusterId) {
			return true
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	return false
}

func CheckRaftStatus(addr string, clusterId, groupId uint) (bool, error) {
	cli, err := redis_cli.NewClient(addr, config.GetAuth(clusterId, ""), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		return true, err
	}

	info, err := cli.MergeInfoV67(uint64(groupId))
	if err != nil {
		return true, err
	}

	if r, ok := info["status"]; ok && r == "true" {
		return true, nil
	} else {
		return false, nil
	}
}

func ParseInfo(info string) map[string]string {
	res := make(map[string]string, 0)
	split := strings.Split(info, "\n")
	for _, v := range split {
		if info := strings.Split(v, ":"); len(info) == 2 {
			res[info[0]] = strings.Trim(info[1], " ")
		}
	}
	return res
}
