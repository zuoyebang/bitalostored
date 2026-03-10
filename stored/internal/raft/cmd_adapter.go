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
	"fmt"
	"time"

	"github.com/zuoyebang/bitalosraft"
)

func AddRaftClusterNode(clusterId uint64, nNodeId uint64, address []byte, retry int) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}

	ret, err := node.AddNode(nNodeId, string(address), retry)
	if R_SUCCESS == ret {
		return nil
	} else {
		return err
	}
}

func AddObserver(clusterId uint64, nNodeId uint64, address []byte, retry int) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}

	ret, err := node.AddObserver(nNodeId, string(address), retry)
	if R_SUCCESS == ret {
		return nil
	} else {
		return err
	}
}

func AddWitness(clusterId uint64, nNodeId uint64, address []byte, retry int) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}
	ret, err := node.AddWitness(nNodeId, string(address), retry)
	if R_SUCCESS == ret {
		return nil
	} else {
		return err
	}
}

func StopRaftClusterNode(clusterId uint64, nNodeId uint64) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}
	return GlobalMultiRaft.StopRaftNode(clusterId, nNodeId)
}

func RemoveRaftClusterNode(clusterId uint64, nNodeId uint64, retry int) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}

	ret, err := node.DelNode(nNodeId, retry)
	if R_SUCCESS == ret {
		return nil
	} else {
		return err
	}
}

func TransferRaftClusterNode(clusterId uint64, targetNodeID uint64) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}
	ret, err := node.LeaderTransfer(targetNodeID)
	if R_SUCCESS == ret {
		return nil
	} else {
		return err
	}
}

func GetLeaderId(clusterId uint64) (uint64, bool, error) {
	if !GlobalMultiRaft.IsOpen() {
		return 0, false, ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return 0, false, errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}
	id, ok, err := node.Nh.GetLeaderID(node.ClusterId)
	if ok {
		return id, true, nil
	}
	return 0, true, err
}

func GetClusterMemberShip(clusterId uint64) (string, error) {
	if !GlobalMultiRaft.IsOpen() {
		return "", ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return "", errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}
	out, ret, err := node.GetClusterMembership()
	if R_SUCCESS == ret {
		return out.Marshal()
	} else {
		return "", err
	}
}

func RemoveRaftNodeData(clusterId uint64, targetNodeID uint64) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}
	ret, err := node.RemoveData(targetNodeID)
	if R_SUCCESS == ret {
		return nil
	} else {
		return err
	}
}

func LogCompact(clusterId uint64) error {
	if !GlobalMultiRaft.IsOpen() {
		return ErrRaftNotOpen
	}
	node := GlobalMultiRaft.GetNodeByClusterId(clusterId)
	if node == nil {
		return errors.New(fmt.Sprintf("raft cluster not exist: %d", clusterId))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	opt := bitalosraft.SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         100000,
	}

	_, err := node.Nh.SyncRequestSnapshot(ctx, node.ClusterId, opt)
	if err != nil {
		return err
	} else {
		return nil
	}
}
