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
	"errors"
	"strings"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/switcher"

	"github.com/gomodule/redigo/redis"
)

type nodeInfo struct {
	status        bool
	currentNodeId string
	startModel    string
	role          string
	isDown        bool
}

type raftGroupInfo struct {
	flagNodeMap map[string]bool
	MasterAddr  string
}

type probeTask struct {
	r *Router

	nodeInfoCache map[string]*nodeInfo
}

func newProbeTask(r *Router) *probeTask {
	return &probeTask{
		r:             r,
		nodeInfoCache: make(map[string]*nodeInfo),
	}
}

func (p *probeTask) doCheck() {
	for i := range p.r.slots {
		p.checkSlotNodeNormal(i)
		p.checkSlotNodeWitness(i)
	}
	p.reset()
}

func (p *probeTask) checkSlotNodeNormal(id int) {
	m := p.r.slots[id].Snapshot(false)
	if len(m.GroupServersCloudMap) <= 0 {
		return
	}

	if m.Switched {
		m.Switched = false
		p.r.FillSlot(m)
		return
	}

	isChangeMaster := false

	rgsi := p.getRaftGroupInfo(m.GroupServersCloudMap)
	if len(rgsi.MasterAddr) <= 0 {
		log.Warnf("groupId:%d slotId:%d not most agree master %s not in GroupServersCloudMap %v",
			m.MasterAddrGroupId, m.Id, rgsi.MasterAddr, m.GroupServersCloudMap)
	} else if rgsi.MasterAddr != m.MasterAddr {
		log.Warnf("groupId:%d slotId:%d master change %s to %s",
			m.MasterAddrGroupId, m.Id, m.MasterAddr, rgsi.MasterAddr)
		m.MasterAddr = rgsi.MasterAddr
		isChangeMaster = true
	}

	if len(rgsi.flagNodeMap) > 0 || isChangeMaster {
		slaveChange := false
		localAddrs := make([]string, 0, 3)
		backupAddrs := make([]string, 0, 3)

		cloudType := p.r.config.ProxyCloudType
		for addr, alive := range rgsi.flagNodeMap {
			if addrCloud, ok := m.GroupServersCloudMap[addr]; ok {
				if alive {
					if addrCloud == cloudType {
						localAddrs = append(localAddrs, addr)
					} else {
						backupAddrs = append(backupAddrs, addr)
					}
				}
			}
		}

		localNewAddrs, localDelAddrs := findAddAndDelAddrs(m.LocalCloudServers, localAddrs)
		backupNewAddrs, backupDelAddrs := findAddAndDelAddrs(m.BackupCloudServers, backupAddrs)

		if len(localNewAddrs) > 0 || len(localDelAddrs) > 0 {
			slaveChange = true
			log.Warnf("groupId:%d slotId:%d local server change %v to %v", m.MasterAddrGroupId, m.Id, m.LocalCloudServers, localAddrs)
			m.LocalCloudServers = localAddrs
		}

		if len(backupNewAddrs) > 0 || len(backupDelAddrs) > 0 {
			slaveChange = true
			log.Warnf("current groupId:%d slotId:%d backup server change %v to %v", m.MasterAddrGroupId, m.Id, m.BackupCloudServers, backupAddrs)
			m.BackupCloudServers = backupAddrs
		}

		if m.GroupServersStats == nil || slaveChange || isChangeMaster {
			m.GroupServersStats = rgsi.flagNodeMap
			p.r.FillSlot(m)
		}
	}
}

func (p *probeTask) checkSlotNodeWitness(id int) {
	m := p.r.slots[id].Snapshot(false)
	for _, addr := range m.WitnessServers {
		p.getNodeInfo(addr, true)
	}
}

func (p *probeTask) getRaftGroupInfo(addrs map[string]string) *raftGroupInfo {
	var hasDeraft bool
	var leaderAddr, deraftAddr string
	flagNodeList := make(map[string]bool)
	pingNodeList := make(map[string]bool)
	nodeIdToAddress := make(map[string]string, len(addrs))

	for addr := range addrs {
		node, err := p.getNodeInfo(addr, false)
		if err == nil {
			if node.status {
				if node.startModel == "normal" {
					nodeIdToAddress[node.currentNodeId] = addr
					pingNodeList[addr] = true
					flagNodeList[addr] = true
					if node.role == "master" {
						leaderAddr = addr
					} else if node.role == "single" && !hasDeraft {
						hasDeraft = true
						deraftAddr = addr
					}
				}
			} else {
				if node.startModel == "normal" {
					pingNodeList[addr] = true
					flagNodeList[addr] = false
				}
			}
		} else {
			flagNodeList[addr] = false
			pingNodeList[addr] = false
		}
	}

	if hasDeraft && len(pingNodeList) == 1 {
		leaderAddr = deraftAddr
	}

	if len(leaderAddr) <= 0 || !switcher.ReadCrossCloud.Load() {
		for addr, pingFlag := range pingNodeList {
			if pingFlag {
				flagNodeList[addr] = true
			}
		}
	}

	rgsi := &raftGroupInfo{
		flagNodeMap: flagNodeList,
		MasterAddr:  leaderAddr,
	}

	return rgsi
}

func (p *probeTask) getNodeInfo(addr string, isWitness bool) (*nodeInfo, error) {
	if nf, exist := p.nodeInfoCache[addr]; exist {
		if nf.isDown {
			return nil, errors.New("node is down")
		}
		return nf, nil
	}

	var addrInfo map[string]string
	var err error
	retry := 2
	for retry > 0 {
		addrInfo, err = p.doInfo(addr)
		if len(addrInfo["status"]) <= 0 {
			time.Sleep(30 * time.Millisecond)
			retry--
			continue
		}
		break
	}

	if err != nil {
		log.Warnf("get info fail addr:%s err:%s", addr, err.Error())
	}

	node := &nodeInfo{}
	if len(addrInfo["status"]) <= 0 {
		log.Warnf("GetNodeInfo not alive [isWitness:%v] [isdown:true] [addr:%s] [data:%v]", isWitness, addr, addrInfo)
		node.isDown = true
		p.nodeInfoCache[addr] = node
		return nil, errors.New("node is down err")
	}

	node.role = addrInfo["role"]
	node.currentNodeId = addrInfo["current_node_id"]
	node.startModel = addrInfo["start_model"]
	if addrInfo["status"] == "true" {
		node.status = true
	} else {
		node.status = false
		log.Warnf("GetNodeInfo not alive [isWitness:%v] [isdown:false] [addr:%s] [data:%v]", isWitness, addr, addrInfo)
	}

	p.nodeInfoCache[addr] = node
	return node, nil
}

func (p *probeTask) reset() {
	p.nodeInfoCache = nil
	p.nodeInfoCache = make(map[string]*nodeInfo)
}

func (p *probeTask) doInfo(addr string) (info map[string]string, err error) {
	info = make(map[string]string)

	pool, ok := p.r.GetAddrPool(addr)
	if !ok {
		err = errors.New("get addr pool empty")
		return
	}

	conn := pool.GetConn()
	defer conn.Close()

	var res string
	res, err = redis.String(conn.Do("INFO", "clusterinfo"))
	if err != nil {
		return
	}

	for _, line := range strings.Split(res, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" {
			info[key] = strings.TrimSpace(kv[1])
		}
	}
	return info, nil
}

func findAddAndDelAddrs(oldSlice, newSlice []string) ([]string, []string) {
	if len(oldSlice) <= 0 {
		return newSlice, nil
	}
	if len(newSlice) <= 0 {
		return nil, oldSlice
	}

	oldMap := make(map[string]string)
	newMap := make(map[string]string)
	addSlice := make([]string, 0, 6)
	delSlice := make([]string, 0, 6)

	for _, addr := range oldSlice {
		oldMap[addr] = addr
	}

	for _, addr := range newSlice {
		newMap[addr] = addr
		if _, exists := oldMap[addr]; !exists {
			addSlice = append(addSlice, addr)
		}
	}

	for addr := range oldMap {
		if _, exists := newMap[addr]; !exists {
			delSlice = append(delSlice, addr)
		}
	}

	return addSlice, delSlice
}
