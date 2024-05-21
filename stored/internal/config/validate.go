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
		c.Server.Keepalive = timesize.Duration(3600 * time.Second)
	}
	if c.Server.SlowTime <= 0 {
		c.Server.SlowTime = timesize.Duration(30 * time.Millisecond)
	}
	if c.Server.Maxclient < 5000 {
		c.Server.Maxclient = 5000
	}
	if c.Server.Maxprocs < 1 {
		c.Server.Maxprocs = 1
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
