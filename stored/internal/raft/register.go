package raft

import (
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/server"
)

const (
	ADD                  string = "add"
	ADD_OBSERVER         string = "addobserver"
	ADD_WITNESS          string = "addwitness"
	REMOVE               string = "remove"
	STOPNODE             string = "stopnode"
	TRANSFER             string = "transfer"
	GET_LEADER           string = "getleader"
	GET_CLUSTER_MEM_SHIP string = "getclustermembership"
	TRANSFER_SLOTS       string = "transferslots"
	ADD_CLUSTER          string = "addcluster"
	REMOVE_SLOTS         string = "removeslots"
	RESET_SLOTS          string = "resetslots"

	SLOTINFO    string = "slotinfo"
	CLUSTERINFO string = "clusterinfo"

	DERAFT     string = "deraft"
	RERAFT     string = "reraft"
	LOGCOMPACT string = "logcompact"
)

func (mr *MultiRaft) registerRaftCommand(s *server.Server) {
	server.AddCommand(map[string]*server.Cmd{
		// master: add address nodeId [clusterId]
		ADD: {Handler: func(c *server.Client) error { return addRaftClusterNode(mr, c) }},
		// master: add_observer address nodeId [clusterId]
		ADD_OBSERVER: {Handler: func(c *server.Client) error { return addObserver(mr, c) }},
		// master: add_witness address nodeId [clusterId]
		ADD_WITNESS: {Handler: func(c *server.Client) error { return addWitness(mr, c) }},
		// remove nodeId [clusterId]
		REMOVE: {Handler: func(c *server.Client) error { return removeRaftClusterNode(mr, c) }},
		// stopnode nodeId [clusterId]
		STOPNODE: {Handler: func(c *server.Client) error { return stopRaftClusterNode(mr, c) }},
		// transfer nodeId [clusterId]
		TRANSFER: {Handler: func(c *server.Client) error { return transferRaftClusterNode(mr, c) }},
		// logcompact [clusterId]
		LOGCOMPACT: {Handler: func(c *server.Client) error { return logCompact(mr, c) }},
		// get_leader [clusterId]
		GET_LEADER: {Handler: func(c *server.Client) error { return getLeaderFrmRaftCluster(mr, c) }},
		// getmembership [clusterId]
		GET_CLUSTER_MEM_SHIP:    {Handler: func(c *server.Client) error { return getClusterMemberShip(mr, c) }},
		SLOTINFO:                {Handler: func(c *server.Client) error { return slotInfo(mr, c) }},
		CLUSTERINFO:             {Handler: func(c *server.Client) error { return clusterInfo(mr, c) }},
		ADD_CLUSTER:             {Handler: func(c *server.Client) error { return addCluster(mr, c) }},
		REMOVE_SLOTS:            {Handler: func(c *server.Client) error { return removeSlots(mr, c) }},
		TRANSFER_SLOTS:          {Handler: func(c *server.Client) error { return tansferSlots(mr, c) }},
		RESET_SLOTS:             {Handler: func(c *server.Client) error { return resetSlots(mr, c) }},
		DERAFT:                  {Handler: func(c *server.Client) error { return deraft(s, mr, c) }},
		RERAFT:                  {Handler: func(c *server.Client) error { return reRaft(s, mr, c) }},
		resp.DERAFT_TO_OBSERVER: {Handler: func(c *server.Client) error { return deraftToObserver(s, mr, c) }},
		resp.DERAFT_TO_WITNESS:  {Handler: func(c *server.Client) error { return deraftToWitness(s, mr, c) }},
	})
}
