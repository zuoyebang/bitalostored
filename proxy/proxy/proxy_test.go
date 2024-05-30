// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"fmt"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var proxyInstance *Proxy
var redisPool *redis.Pool
var storedPool *redis.Pool

func init() {
	newRedisClient()
}

func newProxyTest() *Proxy {
	cfg := config.NewDefaultConfig()
	p, err := New(cfg)
	if err != nil {
		panic(err)
	}
	slots := make([]*models.Slot, 0, 1024)
	for i := 0; i < 1024; i++ {
		slot := &models.Slot{
			Id:                i,
			Locked:            false,
			MasterAddr:        "127.0.0.1:10091",
			MasterAddrGroupId: 1,
			RoundRobinNum:     0,
			LocalCloudServers: []string{
				"127.0.0.1:10091",
				"127.0.0.1:10092",
			},
			BackupCloudServers: []string{
				"127.0.0.1:10093",
			},
			GroupServersCloudMap: map[string]string{
				"127.0.0.1:10091": "baidu",
				"127.0.0.1:10092": "baidu",
				"127.0.0.1:10093": "tencent",
			},
			GroupServersStats: map[string]bool{
				"127.0.0.1:10091": true,
				"127.0.0.1:10092": true,
				"127.0.0.1:10093": true,
			},
		}
		slots[i] = slot
	}
	FillSlots(slots)

	return p
}

func newRedisClient() {
	storedPool = &redis.Pool{
		MaxIdle:     100,
		MaxActive:   100,
		IdleTimeout: 3600 * time.Second,
		Wait:        true,
		Dial: func() (conn redis.Conn, e error) {
			con, err := redis.Dial("tcp", "127.0.0.1:8112")
			if err != nil {
				log.Warnf("get_redis_conn_fail err:%v", err)
				return nil, err
			}
			return con, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func TestProxyDoPing(t *testing.T) {
	proxyInstance = newProxyTest()
	c := storedPool.Get()
	defer c.Close()
	_, err := c.Do("Ping")
	assert.NoError(t, err)
	time.Sleep(30 * time.Minute)
}

func TestProxyDoInfo(t *testing.T) {
	proxyInstance = newProxyTest()
	c := storedPool.Get()
	defer c.Close()
	_, err := c.Do("Ping")
	assert.NoError(t, err)

	if info, err := redis.String(c.Do("INFO")); err != nil {
		fmt.Println("info err:", err)
	} else {
		fmt.Println("info :", info)
	}
	time.Sleep(10 * time.Minute)
}
