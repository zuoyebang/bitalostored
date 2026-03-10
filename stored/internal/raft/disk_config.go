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

package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

var nodeInitDir string

const initSuffixName = "config"

const (
	RaftInitDone = 1
	SlotInitDone = 1
)

func setNodeInitDir(dir string) {
	nodeInitDir = dir
}

type NodeInitConfig struct {
	ClusterId uint64   `toml:"cluster_id" mapstructure:"cluster_id"`
	NodeID    uint64   `toml:"node_id" mapstructure:"node_id"`
	Role      string   `toml:"role" mapstructure:"role"`
	AddrList  []string `toml:"addr_list" mapstructure:"addr_list"`
	NodeList  []uint64 `toml:"node_list" mapstructure:"node_list"`
}

func loadNodeInitConfig() (map[uint64]*NodeInitConfig, error) {
	files, err := os.ReadDir(nodeInitDir)
	if err != nil {
		return nil, err
	}

	nodesConfig := make(map[uint64]*NodeInitConfig, 3)
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), initSuffixName) {
			continue
		}

		c := &NodeInitConfig{}
		filename := filepath.Join(nodeInitDir, f.Name())
		_, err = toml.DecodeFile(filename, c)
		if err != nil {
			log.Errorf("decode raft init config error. file:%s", filename)
			continue
		}
		log.Infof("load config. cluster:%d config:%+v", c.ClusterId, *c)
		nodesConfig[c.ClusterId] = c
	}
	return nodesConfig, nil
}

func (mr *MultiRaft) firstInitV8(c *config.Config) (err error) {
	if mr.raftMeta.GetRaftInit() == RaftInitDone {
		log.Infof("raft v8 is initialized, return")
		return nil
	}

	if err = mr.switchUpdateIndex(); err != nil {
		log.Errorf("switch update index to nodemeta err:%s", err)
		return err
	}

	nodeInit := NodeInitConfig{
		ClusterId: c.RaftCluster.ClusterId,
		NodeID:    c.RaftNodeHost.NodeID,
		Role:      getRole(c.RaftCluster.IsObserver, c.RaftCluster.IsWitness),
		AddrList:  c.RaftNodeHost.InitRaftAddrList,
		NodeList:  c.RaftNodeHost.InitRaftNodeList,
	}
	if err = writeNodeInitConfig(&nodeInit); err == nil {
		log.Infof("set raft init done")
		mr.raftMeta.SetRaftInit(RaftInitDone)
	}
	return err
}

func writeNodeInitConfig(c *NodeInitConfig) error {
	if err := os.MkdirAll(nodeInitDir, 0755); err != nil {
		return err
	}
	filename := filepath.Join(nodeInitDir, getConfigName(c.ClusterId))
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	fp.Truncate(0)
	defer fp.Close()

	e := toml.NewEncoder(fp)
	if err = e.Encode(c); err != nil {
		return err
	}
	return nil
}

func getConfigName(clusterId uint64) string {
	return fmt.Sprintf("raft-%d.%s", clusterId, initSuffixName)
}

func removeNodeInitConfig(clusterId uint64) error {
	return os.Remove(filepath.Join(nodeInitDir, getConfigName(clusterId)))
}
