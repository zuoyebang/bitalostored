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
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	braft "github.com/zuoyebang/bitalostored/raft"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/server"
)

func addRaftClusterNode(raft *StartRun, c *server.Client) error {
	if len(c.Args) != 2 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	ret, err := raft.AddNode(nNodeId, unsafe2.String(c.Args[0]), raft.RetryTimes)
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func addObserver(raft *StartRun, c *server.Client) error {
	if len(c.Args) != 2 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	ret, err := raft.AddObserver(nNodeId, unsafe2.String(c.Args[0]))
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func addWitness(raft *StartRun, c *server.Client) error {
	if len(c.Args) != 2 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	ret, err := raft.AddWitness(nNodeId, unsafe2.String(c.Args[0]))
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func removeRaftClusterNode(raft *StartRun, c *server.Client) error {
	if len(c.Args) != 1 {
		return errn.ErrLenArg
	}

	nNodeId, err := strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}

	ret, err := raft.DelNode(nNodeId, raft.RetryTimes)
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func transferRaftClusterNode(raft *StartRun, c *server.Client) error {
	if len(c.Args) != 1 {
		return errn.ErrLenArg
	}

	targetNodeID, err := strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if nil != err {
		return errn.ErrSyntax
	}

	ret, err := raft.LeaderTransfer(targetNodeID)
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func getLeaderFrmRaftCluster(raft *StartRun, c *server.Client) error {
	id, ret, err := raft.GetLeaderId()
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(strconv.FormatUint(id, 10))
		return nil
	} else {
		return err
	}
}

func getNodeHostInfo(raft *StartRun, c *server.Client) error {
	out, ret, err := raft.GetNodeHostInfo()
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(out)
		return nil
	} else {
		return err
	}
}

func getClusterMemberShip(raft *StartRun, c *server.Client) error {
	out, ret, err := raft.GetClusterMembership()
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus(out)
		return nil
	} else {
		return err
	}
}

func removeRaftNodeData(raft *StartRun, c *server.Client) error {
	if len(c.Args) != 1 {
		return errn.ErrLenArg
	}
	targetNodeID, err := strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if nil != err {
		return errn.ErrSyntax
	}
	ret, err := raft.RemoveData(targetNodeID)
	if ret == R_SUCCESS {
		c.RespWriter.WriteStatus("remove data request sent successfully !")
		return nil
	} else {
		return err
	}
}

func deraft(s *server.Server, raft *StartRun, c *server.Client) error {
	if len(c.Args) != 1 {
		return resp.CmdParamsErr(DERAFT)
	}

	if string(c.Args[0]) != config.GlobalConfig.Server.Token {
		return errors.New("degrade valid token err")
	}
	raft.Mu.Lock()
	defer raft.Mu.Unlock()

	ret, err := raft.StopNodeHost()
	if ret != R_SUCCESS {
		return err
	}
	s.GetDB().RaftReset()
	if err := config.GlobalConfig.SetDegradeSingleNode(); err != nil {
		c.RespWriter.WriteError(err)
	} else {
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}
	return nil
}

func reRaft(s *server.Server, raft *StartRun, c *server.Client) error {
	if len(c.Args) != 2 {
		return resp.CmdParamsErr(RERAFT)
	}

	if string(c.Args[0]) != config.GlobalConfig.Server.Token {
		return errors.New("valid token err")
	}

	raft.Mu.Lock()
	defer raft.Mu.Unlock()

	if raft.RaftReady {
		return errors.New("raft is ok")
	}

	s.GetDB().RaftReset()

	port := string(c.Args[1])
	var err error
	if err = raft.Clean(); err == nil {
		if err = ReraftInit(s, port); err == nil {
			if err = config.GlobalConfig.WriteFile(config.GlobalConfig.Server.ConfigFile); err == nil {
				c.RespWriter.WriteStatus(resp.ReplyOK)
			}
		}
	}
	if err != nil {
		c.RespWriter.WriteError(err)
	}
	return err
}

func logCompact(raft *StartRun, c *server.Client) error {
	raft.Mu.Lock()
	defer raft.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	opt := braft.SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         100000,
	}

	_, err := raft.Nh.SyncRequestSnapshot(ctx, raft.ClusterId, opt)
	if err != nil {
		c.RespWriter.WriteError(err)
	} else {
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}
	return err
}

func okNodeHost(raft *StartRun, c *server.Client) error {
	ok, ret, err := raft.GetOK()
	if R_SUCCESS == ret {
		var sRet string
		if ok {
			sRet = "true"
		} else {
			sRet = "false"
		}
		c.RespWriter.WriteStatus(sRet)
	} else {
		return err
	}
	return nil
}

func fullSync(raft *StartRun, c *server.Client) error {
	if c.IsMaster() {
		return errors.New("master not need full sync")
	}
	err := raft.FullSync()
	if err == nil {
		c.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	}
	return err
}

func statInfo(raft *StartRun, c *server.Client) error {
	info, ret, err := raft.StatInfo()
	if R_SUCCESS == ret {
		c.RespWriter.WriteStatus(info)
	} else {
		return err
	}
	return nil
}
