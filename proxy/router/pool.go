// Copyright 2019 The Bitalostored author and other contributors.
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

package router

import (
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"

	"github.com/gomodule/redigo/redis"
)

type InternalPool struct {
	HostPort string
	Pool     *redis.Pool
}

type InternalPoolStat struct {
	ActiveCount int
	IdleCount   int
}

func (p *InternalPool) GetConn() redis.Conn {
	conn := p.Pool.Get()
	return conn
}

func (p *InternalPool) GetHostPort() string {
	return p.HostPort
}

func (p *InternalPool) PoolClose() {
	p.Pool.Close()
}

func (p *InternalPool) Stats() InternalPoolStat {
	s := p.Pool.Stats()
	return InternalPoolStat{
		ActiveCount: s.ActiveCount,
		IdleCount:   s.IdleCount,
	}
}

func GetPool(conf models.RedisConnConf) *redis.Pool {
	return &redis.Pool{
		MaxIdle:         conf.MaxIdle,
		MaxActive:       conf.MaxActive,
		IdleTimeout:     conf.IdleTimeout.Duration(),
		MaxConnLifetime: conf.ConnLifeTime.Duration(),
		Wait:            true,
		Dial: func() (conn redis.Conn, e error) {
			conn, err := redis.Dial("tcp", conf.HostPort,
				redis.DialPassword(conf.Password),
				redis.DialDatabase(conf.DataBase),
				redis.DialConnectTimeout(conf.ConnTimeout.Duration()),
				redis.DialReadTimeout(conf.ReadTimeout.Duration()),
				redis.DialWriteTimeout(conf.WriteTimeout.Duration()),
			)
			if err != nil {
				log.Warn("get_redis_conn_fail: ", err)
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
