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
	"bytes"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/cmd"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
	"github.com/zuoyebang/bitalostored/stored/server"
)

// add address nodeId [clusterId]
func addRaftClusterNode(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 2 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	var n int
	var clusterId uint64
	if len(c.Args) == 3 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[2]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}

	err = AddRaftClusterNode(clusterId, nNodeId, c.Args[0], mr.retryTimes)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

// add_observer address nodeId [clusterId]
func addObserver(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 2 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	var clusterId uint64
	if len(c.Args) == 3 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[2]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		var n int
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}

	err = AddObserver(clusterId, nNodeId, c.Args[0], mr.retryTimes)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

// add_witness address nodeId [clusterId]
func addWitness(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 2 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	var n int
	var clusterId uint64
	if len(c.Args) == 3 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[2]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}

	err = AddWitness(clusterId, nNodeId, c.Args[0], mr.retryTimes)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

// remove nodeId [clusterId]
func removeRaftClusterNode(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 1 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	var n int
	var clusterId uint64
	if len(c.Args) == 2 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}

	err = RemoveRaftClusterNode(clusterId, nNodeId, mr.retryTimes)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

// stopnode nodeId [clusterId]
func stopRaftClusterNode(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 1 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	var n int
	var clusterId uint64
	if len(c.Args) == 2 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}

	err = StopRaftClusterNode(clusterId, nNodeId)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

// transfer nodeId [clusterId]
func transferRaftClusterNode(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 1 {
		return errn.ErrLenArg
	}

	targetNodeID, err := strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if nil != err {
		return errn.ErrSyntax
	}

	var n int
	var clusterId uint64
	if len(c.Args) == 2 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}

	err = TransferRaftClusterNode(clusterId, targetNodeID)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

func getLeaderFrmRaftCluster(mr *MultiRaft, c *server.Client) error {
	var clusterId uint64
	id, ret, err := GetLeaderId(clusterId)
	if ret {
		c.Writer.WriteStatus(strconv.FormatUint(id, 10))
		return nil
	} else {
		return err
	}
}

// addcluster clusterId nodeId nodeAddrList nodeIdList
// nodeAddrList: 1.1.1.1:10001,1.1.1.1:10002
// nodeIdList: 1,2
func addCluster(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 4 {
		return errn.ErrArgsEmpty
	}
	var err error
	var clusterId, nodeId uint64
	clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	nodeId, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	nodeAddrList := string(c.Args[2])
	addrs := strings.Split(nodeAddrList, ",")
	nodeAddrs := make([]string, 0, len(nodeAddrList))
	for _, addr := range addrs {
		nodeAddrs = append(nodeAddrs, addr)
	}

	nodeIdList := strings.Split(unsafe2.String(c.Args[3]), ",")
	nodeIds := make([]uint64, 0, len(nodeIdList))
	for _, id := range nodeIdList {
		if nid, err := strconv.Atoi(id); err != nil {
			return errn.ErrSyntax
		} else {
			nodeIds = append(nodeIds, uint64(nid))
		}
	}

	initConfig := &NodeInitConfig{
		ClusterId: clusterId,
		NodeID:    nodeId,
		Role:      "normal",
		AddrList:  nodeAddrs,
		NodeList:  nodeIds,
	}
	_, err = mr.StartRaftNode(initConfig, true)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}

// removeSlots slotA slotB authToken
func removeSlots(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 3 {
		return errn.ErrArgsEmpty
	}
	var err error
	var slotBegin, slotEnd uint64
	slotBegin, err = strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	slotEnd, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	if !checkSlot(uint16(slotBegin)) || !checkSlot(uint16(slotEnd)) {
		return errors.New("slot not allowed")
	}
	if slotEnd < slotBegin {
		return errors.New("slot not allowed")
	}
	token := unsafe2.String(c.Args[2])
	if token != mr.cfg.Server.Token {
		return errors.New("valid token err")
	}

	log.Infof("remove slots begin:%d end:%d", slotBegin, slotEnd)
	mr.mapping.removeSlots(uint16(slotBegin), uint16(slotEnd))
	mr.s.GetMeta().MarkSlotRemoved(int(slotBegin), int(slotEnd))
	mr.s.RemoveSlots(true)
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

// slotA slotB targetGroupId
func tansferSlots(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) < 3 {
		return errn.ErrArgsEmpty
	}
	var err error
	var slotBegin, slotEnd, targetGroupId uint64
	slotBegin, err = strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	slotEnd, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	if !checkSlot(uint16(slotBegin)) || !checkSlot(uint16(slotEnd)) {
		return errors.New("slot not allowed")
	}
	if slotEnd < slotBegin {
		return errors.New("slot not allowed")
	}
	targetGroupId, err = strconv.ParseUint(unsafe2.String(c.Args[2]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	if err := mr.SetOnlineGroups(map[uint64]bool{targetGroupId: true}); err != nil {
		return err
	}
	_, _, err = mr.mapping.updateGroups(uint16(slotBegin), uint16(slotEnd), targetGroupId)
	if err != nil {
		return err
	}
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

func resetSlots(mr *MultiRaft, c *server.Client) error {
	var err error
	num, gid := mr.getClusterNum()
	if num != 1 {
		return errors.New("reset error: multi raft cluster")
	}
	if _, _, err = mr.mapping.updateGroups(uint16(0), uint16(btools.TotalSlotMask), gid); err != nil {
		return err
	}
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

func getClusterMemberShip(mr *MultiRaft, c *server.Client) error {
	var err error
	var n int
	var clusterId uint64
	if len(c.Args) == 1 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n != 1 {
			return ErrClusterMiss
		}
	}
	out, err := GetClusterMemberShip(clusterId)
	if err == nil {
		c.Writer.WriteStatus(out)
		return nil
	} else {
		return err
	}
}

func slotInfo(mr *MultiRaft, c *server.Client) error {
	if mr.IsOpen() {
		slots := make([]*SlotGroupInfo, 0, 1024)

		for sid, gid := range mr.mapping.slotGroup {
			slots = append(slots, &SlotGroupInfo{slot: sid, group: gid})
		}
		sort.Slice(slots, func(i, j int) bool {
			return slots[i].slot < slots[j].slot
		})

		buf := make([]byte, 0, 1024*9)
		nb := bytes.NewBuffer(buf)
		for _, s := range slots {
			nb.WriteString(strconv.Itoa(int(s.slot)) + ":" + strconv.Itoa(int(s.group)) + "\n")
		}
		c.Writer.WriteBulk(nb.Bytes())
	} else {
		c.Writer.WriteBulk(nil)
	}
	return nil
}

func clusterInfo(mr *MultiRaft, c *server.Client) error {
	if len(c.Args) == 0 {
		res, closers, err := mr.GetAllRaftInfo()
		defer func() {
			if closers != nil {
				for _, f := range closers {
					f()
				}
			}
		}()
		if err != nil {
			return err
		}
		c.Writer.WriteBulk(res)
	} else {
		n, err := strconv.Atoi(unsafe2.String(c.Args[0]))
		if err != nil {
			return err
		}
		clusterId := uint64(n)
		info, closer := mr.GetRaftInfo(clusterId)
		c.Writer.WriteBulk(info)
		if closer != nil {
			closer()
		}
	}
	return nil
}

func deraft(s *server.Server, mr *MultiRaft, c *server.Client) error {
	if len(c.Args) != 1 {
		return errn.CmdParamsErr(DERAFT)
	}

	if string(c.Args[0]) != config.GlobalConfig.Server.Token {
		return errors.New("deraft valid token err")
	}

	if !mr.deraftFlag.CompareAndSwap(0, 1) {
		return errors.New("reraft or deraft is doing")
	}
	defer mr.deraftFlag.Store(0)

	n, _ := mr.getClusterNum()
	if n != 1 {
		return errors.New("raft num is not 1")
	}

	if err := mr.clear(); err != nil {
		return err
	}

	s.GetDB().RaftReset()
	if err := config.GlobalConfig.SetDegradeSingleNode(); err == nil {
		mr.deraftInit()
		c.Writer.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func reRaft(s *server.Server, mr *MultiRaft, c *server.Client) error {
	if len(c.Args) != 2 {
		return errn.CmdParamsErr(RERAFT)
	}

	if string(c.Args[0]) != config.GlobalConfig.Server.Token {
		return errors.New("valid token err")
	}

	if !mr.deraftFlag.CompareAndSwap(0, 1) {
		return errors.New("reraft or deraft is doing")
	}
	defer mr.deraftFlag.Store(0)

	if !config.GlobalConfig.CheckIsDegradeSingleNode() {
		return errors.New("node is not derafted. ignore reraft")
	}

	s.GetDB().RaftReset()

	port := string(c.Args[1])
	config.GlobalConfig.RaftNodeHost.RaftAddress = utils.GetLocalIp() + ":" + port
	config.GlobalConfig.RaftNodeHost.InitRaftAddrList = []string{config.GlobalConfig.RaftNodeHost.RaftAddress}
	config.GlobalConfig.RaftNodeHost.InitRaftNodeList = []uint64{config.GlobalConfig.RaftNodeHost.NodeID}
	config.GlobalConfig.RaftCluster.IsObserver = false
	config.GlobalConfig.RaftCluster.IsWitness = false
	config.GlobalConfig.ConvertRaft()

	err := startNodehost(config.GlobalConfig, s)
	if err == nil {
		log.Infof("reraft init done")
		GlobalMultiRaft.deraftClean()
		config.GlobalConfig.Plugin.OpenRaft = true
		config.GlobalConfig.Server.DegradeSingleNode = false
		if err = config.GlobalConfig.WriteFile(config.GlobalConfig.Server.ConfigFile); err == nil {
			c.Writer.WriteStatus(resp.ReplyOK)
		}
	} else {
		log.Infof("reraft init err:%s", err)
	}
	return err
}

func deraftToObserver(s *server.Server, mr *MultiRaft, c *server.Client) error {
	if len(c.Args) != 4 {
		return errn.CmdParamsErr(resp.DERAFT_TO_OBSERVER)
	}

	if !cmd.CheckCmdToken(string(c.Args[0])) {
		return errn.ErrCmdToken
	}

	port := string(c.Args[1])
	masterAddr := string(c.Args[2])
	if len(port) == 0 || len(masterAddr) == 0 {
		return errn.CmdParamsErr(resp.DERAFT_TO_OBSERVER)
	}
	nodeId, err := strconv.Atoi(unsafe2.String(c.Args[3]))
	if err != nil {
		return err
	}

	if !mr.deraftFlag.CompareAndSwap(0, 1) {
		return errors.New("deraftToObserver is doing")
	}
	defer mr.deraftFlag.Store(0)

	if err = mr.clear(); err != nil {
		return err
	}

	s.GetDB().RaftReset()

	newAddr := utils.GetLocalIp() + ":" + port
	config.GlobalConfig.RaftNodeHost.RaftAddress = newAddr
	config.GlobalConfig.RaftNodeHost.InitRaftAddrList = []string{masterAddr}
	config.GlobalConfig.RaftNodeHost.InitRaftNodeList = []uint64{uint64(nodeId)}
	config.GlobalConfig.RaftCluster.IsObserver = true
	config.GlobalConfig.RaftCluster.IsWitness = false
	config.GlobalConfig.ConvertRaft()

	if err = startNodehost(config.GlobalConfig, s); err != nil {
		log.Errorf("[CMD] deraftToObserver startNodehost err:%s", err)
		return err
	}

	if err = config.GlobalConfig.WriteFile(config.GlobalConfig.Server.ConfigFile); err != nil {
		log.Errorf("[CMD] deraftToObserver rewrite config err:%s", err)
		return err
	}

	log.Infof("[CMD] deraftToObserver success nodeId:%d newAddr:%s masterAddr:%s", nodeId, newAddr, masterAddr)
	c.Writer.WriteStatus(resp.ReplyOK)
	return err
}

func deraftToWitness(s *server.Server, mr *MultiRaft, c *server.Client) error {
	if len(c.Args) != 4 {
		return errn.CmdParamsErr(resp.DERAFT_TO_WITNESS)
	}

	if !cmd.CheckCmdToken(string(c.Args[0])) {
		return errn.ErrCmdToken
	}

	port := string(c.Args[1])
	masterAddr := string(c.Args[2])
	if len(port) == 0 || len(masterAddr) == 0 {
		return errn.CmdParamsErr(resp.DERAFT_TO_WITNESS)
	}
	nodeId, err := strconv.Atoi(unsafe2.String(c.Args[3]))
	if err != nil {
		return err
	}

	if !mr.deraftFlag.CompareAndSwap(0, 1) {
		return errors.New("deraftToWitness is doing")
	}
	defer mr.deraftFlag.Store(0)

	if err = mr.clear(); err != nil {
		return err
	}

	newAddr := utils.GetLocalIp() + ":" + port
	config.GlobalConfig.RaftNodeHost.RaftAddress = newAddr
	config.GlobalConfig.RaftNodeHost.InitRaftAddrList = []string{masterAddr}
	config.GlobalConfig.RaftNodeHost.InitRaftNodeList = []uint64{uint64(nodeId)}
	config.GlobalConfig.RaftCluster.IsObserver = false
	config.GlobalConfig.RaftCluster.IsWitness = true
	config.GlobalConfig.ConvertRaft()

	if err = startNodehost(config.GlobalConfig, s); err != nil {
		log.Errorf("[CMD] deraftToWitness startNodehost err:%s", err)
		return err
	}

	if err = config.GlobalConfig.WriteFile(config.GlobalConfig.Server.ConfigFile); err != nil {
		log.Errorf("[CMD] deraftToWitness rewrite config err:%s", err)
		return err
	}

	log.Infof("[CMD] deraftToWitness success nodeId:%d newAddr:%s masterAddr:%s", nodeId, newAddr, masterAddr)
	c.Writer.WriteStatus(resp.ReplyOK)
	return err
}

func logCompact(mr *MultiRaft, c *server.Client) error {
	if !mr.logCompact.CompareAndSwap(0, 1) {
		return errors.New("log compact is doing")
	}
	defer mr.logCompact.Store(0)

	var err error
	var n int
	var clusterId uint64
	if len(c.Args) == 1 {
		clusterId, err = strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
		if err != nil {
			return errn.ErrSyntax
		}
	} else {
		n, clusterId = mr.getClusterNum()
		if n == 0 {
			c.Writer.WriteStatus(resp.ReplyOK)
			return nil
		} else if n != 1 {
			return ErrClusterMiss
		}
	}
	err = LogCompact(clusterId)
	if err == nil {
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return err
}
