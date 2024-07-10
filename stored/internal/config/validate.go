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

package config

import (
	"errors"
	"os"
	"path"
	"time"

	"github.com/zuoyebang/bitalostored/butils/bytesize"
	"github.com/zuoyebang/bitalostored/butils/timesize"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const (
	MinProcs           = 2
	MaxProcs           = 40
	MinCores           = 1
	MaxCores           = 20
	MinNetEventLoopNum = 8
	MaxNetEventLoopNum = 256
)

func (c *Config) Validate() error {
	if err := c.checkServerConfig(); err != nil {
		return err
	}
	if err := c.checkLogConfig(); err != nil {
		return err
	}
	if err := c.checkBitalosConfig(); err != nil {
		return err
	}
	if err := c.checkSlidingWindowConfig(); err != nil {
		return err
	}
	if err := c.checkLRUCacheConfig(); err != nil {
		return nil
	}
	if err := c.checkRaftClusterConfig(); err != nil {
		return err
	}
	return nil
}

func (c *Config) checkServerConfig() error {
	if c.Server.Address == "" {
		return errors.New("invalid server address")
	}

	if c.Server.DBPath == "" {
		return errors.New("invalid server dbpath")
	} else if !path.IsAbs(c.Server.DBPath) {
		baseDir, _ := os.Getwd()
		c.Server.DBPath = path.Join(baseDir, c.Server.DBPath)
	}

	if c.Server.Keepalive <= 0 {
		c.Server.Keepalive = timesize.Duration(1800 * time.Second)
	}
	if c.Server.SlowTime <= 0 {
		c.Server.SlowTime = timesize.Duration(30 * time.Millisecond)
	}
	if c.Server.Maxclient < 5000 {
		c.Server.Maxclient = 5000
	}
	if c.Server.Maxprocs < MinProcs {
		c.Server.Maxprocs = MinProcs
	}
	if c.Server.Maxprocs > MaxProcs {
		c.Server.Maxprocs = MaxProcs
	}
	if c.Server.NetEventLoopNum < c.Server.Maxprocs*2 {
		c.Server.NetEventLoopNum = c.Server.Maxprocs * 2
	}
	if c.Server.NetEventLoopNum < MinNetEventLoopNum {
		c.Server.NetEventLoopNum = MinNetEventLoopNum
	}
	if c.Server.NetEventLoopNum > MaxNetEventLoopNum {
		c.Server.NetEventLoopNum = MaxNetEventLoopNum
	}

	return nil
}

func (c *Config) checkBitalosConfig() error {
	var miniWriteBuffer, maxWriteBuffer bytesize.Int64

	miniWriteBuffer.UnmarshalText([]byte("256mb"))
	maxWriteBuffer.UnmarshalText([]byte("4gb"))

	if c.Bitalos.WriteBufferSize < miniWriteBuffer {
		c.Bitalos.WriteBufferSize = miniWriteBuffer
	}
	if c.Bitalos.WriteBufferSize > maxWriteBuffer {
		c.Bitalos.WriteBufferSize = maxWriteBuffer
	}

	return nil
}

func (c *Config) checkLogConfig() error {
	if !log.CheckRotation(c.Log.RotationTime) {
		c.Log.RotationTime = log.DailyRotate
	}
	return nil
}

func (c *Config) checkSlidingWindowConfig() error {
	return nil
}

func (c *Config) checkLRUCacheConfig() error {
	return nil
}

func (c *Config) checkRaftClusterConfig() error {
	return nil
}
