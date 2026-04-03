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

package uredis

import (
	"bytes"
	"container/list"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/butils/math2"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/utils"
)

type Client struct {
	conn redigo.Conn
	Addr string
	Auth string

	Database int

	LastUse time.Time
}

type NodeInfo struct {
	NodeStatus     bool // true or false
	CurrentNodeId  string
	CurrentAddress string
	StartModel     string // normal , observer, witness
	Role           string // master, slave, witness
	ClusterId      string
	LeaderNodeId   string
	LeaderAddress  string
	ClusterNodes   string

	isDown bool
}

func (nf *NodeInfo) Md5() string {
	buf := bytes.NewBuffer(make([]byte, 0, 28))
	buf.WriteString("md5_raft_node")
	buf.WriteString(nf.ClusterId)
	buf.WriteString(nf.LeaderNodeId)
	buf.WriteString(nf.ClusterNodes)
	return fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
}

func NewClientNoAuth(addr string, timeout time.Duration) (*Client, error) {
	return NewClient(addr, "", timeout)
}

func NewClient(addr string, auth string, timeout time.Duration) (*Client, error) {
	c, err := redigo.Dial("tcp", addr, []redigo.DialOption{
		redigo.DialConnectTimeout(math2.MinDuration(200*time.Millisecond, timeout)),
		redigo.DialPassword(auth),
		redigo.DialReadTimeout(math2.MinDuration(10*time.Second, timeout)),
		redigo.DialWriteTimeout(math2.MinDuration(10*time.Second, timeout)),
	}...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Client{
		conn: c, Addr: addr, Auth: auth,
		LastUse: time.Now(),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	r, err := c.conn.Do(cmd, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) Receive() (interface{}, error) {
	r, err := c.conn.Receive()
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) Shutdown() error {
	_, err := c.Do("SHUTDOWN")
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) LogCompact(gid int) error {
	_, err := c.Do("setex", "test@#$!stored_logcompact", 1, "1")
	if err != nil {
		return err
	}
	_, err = c.Do("logcompact", gid)
	if err == nil {
		c.Close()
		return nil
	}
	_, err = c.Do("logcompact")
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) SlotInfo() (string, error) {
	text, err := redigo.String(c.Do("slotinfo"))
	if err != nil {
		return "", errors.Trace(err)
	}
	return text, nil
}

func (c *Client) DiskInfo() (map[string]string, error) {
	text, err := redigo.String(c.Do("diskinfo"))
	if err != nil {
		return nil, err
	}
	return utils.ConvertInfoMap(text), nil
}

func (c *Client) AllClusterInfo() (string, error) {
	text, err := redigo.String(c.Do("clusterinfo"))
	if err != nil {
		return "", errors.Trace(err)
	}
	return text, nil
}

func (c *Client) Info(gid int) (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	t1 := utils.ConvertInfoMap(text)
	// INFO command do not return "status" field when version >= v7
	if _, ok := t1["status"]; ok {
		return t1, nil
	}
	if gid == 0 {
		return t1, nil
	}

	text2, _ := redigo.String(c.Do("clusterinfo", gid))
	if len(text2) <= 0 {
		return t1, nil
	}
	t2 := utils.ConvertInfoMap(text2)
	if len(t2) > 0 {
		for k, v := range t2 {
			t1[k] = v
		}
	}
	return t1, nil
}

func (c *Client) GetMajorVersion() int {
	sinfo, err := c.SimpleInfo()
	if err != nil {
		log.WarnErrorf(err, "get info fail(%s). err:%s", c.Addr, err)
		return 0
	}

	majorVersion := 0
	if v, ok := sinfo["major_version"]; ok {
		majorVersion = utils.GetMajorVersion(v)
	}
	return majorVersion
}

func (c *Client) SimpleInfo() (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	info := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
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

func (c *Client) DebugInfo() (map[string]string, error) {
	text, err := redigo.String(c.Do("DEBUGINFO"))

	if err != nil {
		return nil, errors.Trace(err)
	}
	info := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
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

func (c *Client) InfoFull(gid int) (map[string]string, error) {
	if info, err := c.Info(gid); err != nil {
		return nil, errors.Trace(err)
	} else {
		host := info["master_host"]
		port := info["master_port"]
		if host != "" || port != "" {
			info["master_addr"] = net.JoinHostPort(host, port)
		}
		info["maxmemory"] = strconv.Itoa(268435456)
		return info, nil
	}
}

func (c *Client) InfoV7WithRaft(gid int) (map[string]string, error) {
	info := make(map[string]string, 1000)
	if gid > 0 {
		allClusters, _ := c.AllClusterInfo()
		info["all_cluster"] = allClusters
		slotInfo, _ := c.SlotInfo()
		info["slots"] = slotInfo
		disks, _ := c.DiskInfo()
		for k, v := range disks {
			info[k] = v
		}
		return info, nil
	} else {
		return info, nil
	}
}

func (c *Client) InfoWithRaft(gid int) (map[string]string, error) {
	if info, err := c.Info(gid); err != nil {
		return nil, errors.Trace(err)
	} else if gid > 0 {
		allClusters, _ := c.AllClusterInfo()
		info["all_cluster"] = allClusters
		slotInfo, _ := c.SlotInfo()
		info["slots"] = slotInfo
		return info, nil
	} else {
		return info, nil
	}
}

func (c *Client) DebugInfoFull() (map[string]string, error) {
	if info, err := c.DebugInfo(); err != nil {
		return nil, errors.Trace(err)
	} else {
		return info, nil
	}
}

func (c *Client) GetNodeStatus(gid int) (string, error) {
	if info, err := c.Info(gid); err != nil {
		return "", errors.Trace(err)
	} else {
		currentStatus := info["status"]
		currentRaftNodeId := info["current_node_id"]
		if currentStatus == "true" && len(currentRaftNodeId) > 0 {
			return currentRaftNodeId, nil
		}
	}
	return "", errors.New("node status false")
}

func (c *Client) PromoteMasterV6(currentRaftNodeId string, gid int) error {
	var err error
	var ok string
	ok, err = redigo.String(c.Do("transfer", currentRaftNodeId))
	log.Infof("addr:%s cmd:transfer node:%s gid:%d", c.Addr, currentRaftNodeId, gid)
	if err == nil && strings.ToLower(ok) == "ok" {
		return nil
	} else if err != nil {
		return err
	} else {
		return errors.New(fmt.Sprintf("do promote server err, master : %s repley : %s", c.Addr, ok))
	}
}

func (c *Client) PromoteMasterV7(currentRaftNodeId string, gid int) error {
	var err error
	var ok string
	ok, err = redigo.String(c.Do("transfer", currentRaftNodeId, gid))
	log.Infof("addr:%s cmd:transfer node:%s gid:%d", c.Addr, currentRaftNodeId, gid)
	if err == nil && strings.ToLower(ok) == "ok" {
		return nil
	} else if err != nil {
		return err
	} else {
		return errors.New(fmt.Sprintf("do promote server err, master : %s repley : %s", c.Addr, ok))
	}
}

func (c *Client) MigrateStatus(slotId int) ([]byte, error) {
	if reply, err := redigo.Bytes(c.Do("MIGRATESTATUS", slotId)); err != nil {
		return nil, errors.Trace(err)
	} else {
		return reply, nil
	}
}

func (c *Client) ShutDown() error {
	if _, err := c.Do("shutdown"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) MigrateEnd(slotId int) error {
	if _, err := c.Do("MIGRATEEND", slotId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// addobserver localhost:64004 4
func (c *Client) AddObserverV6(raftAddress string, nodeId, gid int) error {
	if _, err := c.Do("addobserver", raftAddress, nodeId, gid); err == nil {
		return nil
	}
	if _, err := c.Do("addobserver", raftAddress, nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddObserverV7(raftAddress string, nodeId, gid int) error {
	if _, err := c.Do("addobserver", raftAddress, nodeId, gid); err == nil {
		return nil
	}
	if _, err := c.Do("addobserver", raftAddress, nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// addwitness localhost:64004 4
func (c *Client) AddWitnessV6(raftAddress string, nodeId, gid int) error {
	if _, err := c.Do("addwitness", raftAddress, nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddWitnessV7(raftAddress string, nodeId, gid int) error {
	if _, err := c.Do("addwitness", raftAddress, nodeId, gid); err == nil {
		return nil
	} else {
		return errors.Trace(err)
	}
}

// v7: stopnode nodeId [clusterId]
func (c *Client) StopNodeV7(nodeId, gid int) error {
	if _, err := c.Do("stopnode", nodeId, gid); err == nil {
		return nil
	} else if strings.Contains(err.Error(), "empty command") {
		return nil
	} else {
		return err
	}
}

// remove 4
func (c *Client) RemoveRaftNodeV6(nodeId, gid int) error {
	if _, err := c.Do("remove", nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) RemoveRaftNodeV7(nodeId, gid int) error {
	if _, err := c.Do("remove", nodeId, gid); err == nil {
		return nil
	} else {
		return errors.Trace(err)
	}
}

func (c *Client) GetClusterMemberShipV7(gid int) (*MembershipV2, error) {
	var data []byte
	var err error
	if data, err = redigo.Bytes(c.Do("getclustermembership", gid)); err != nil {
		return nil, errors.Trace(err)
	}
	membership := &MembershipV2{}
	if err := json.Unmarshal(data, membership); err != nil {
		return nil, err
	}
	return membership, nil
}

func (c *Client) GetClusterMemberShipV6(gid int) (*MembershipV2, error) {
	var data []byte
	var err error
	if data, err = redigo.Bytes(c.Do("getclustermembership")); err != nil {
		return nil, errors.Trace(err)
	}
	membership := &MembershipV2{}
	if err := json.Unmarshal(data, membership); err != nil {
		return nil, err
	}
	return membership, nil
}

func (c *Client) DeRaft(gid int, token string) error {
	if _, err := c.Do("deraft", token); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) ReRaft(token string, port int) error {
	var err error
	if _, err = c.Do("reraft", token, port); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Compact(dbType string) error {
	var err error
	if _, err = c.Do("compact", dbType); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AutoCompact(value int) error {
	var err error
	if _, err = c.Do("config", "SET", "AUTOCOMPACT", value); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) GetNodeHostInfo() (string, error) {
	if data, err := redigo.String(c.Do("getnodehostinfo")); err != nil {
		return "", errors.Trace(err)
	} else {
		return data, nil
	}
}

// add localhost:64004 4
func (c *Client) AddToSlaveV7(raftAddress string, nodeId, gid int) error {
	if _, err := c.Do("add", raftAddress, nodeId, gid); err == nil {
		return nil
	} else {
		return errors.Trace(err)
	}

}

func (c *Client) AddToSlaveV6(raftAddress string, nodeId, gid int) error {
	if _, err := c.Do("add", raftAddress, nodeId); err == nil {
		return nil
	} else {
		return errors.Trace(err)
	}
}

func (c *Client) MigrateSlots(slotid int, target string) error {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := c.Do("MIGRATESLOTS", host, port, slotid); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) TransferSlots(sid, eid, target int) error {
	if _, err := c.Do("TRANSFERSLOTS", sid, eid, target); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) RemoveSlots(sid, eid int, token string) error {
	if _, err := c.Do("removeslots", sid, eid, token); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) ResetSlots() error {
	if _, err := c.Do("resetslots"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type MigrateSlotAsyncOption struct {
	MaxBulks int
	MaxBytes int
	NumKeys  int
	Timeout  time.Duration
}

func (c *Client) SlotsInfo() (map[int]int, error) {
	if reply, err := c.Do("SLOTSINFO"); err != nil {
		return nil, errors.Trace(err)
	} else {
		infos, err := redigo.Values(reply, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		slots := make(map[int]int)
		for i, info := range infos {
			p, err := redigo.Ints(info, nil)
			if err != nil || len(p) != 2 {
				return nil, errors.Errorf("invalid response[%d] = %v", i, info)
			}
			slots[p[0]] = p[1]
		}
		return slots, nil
	}
}

func (c *Client) Role() (string, error) {
	if reply, err := c.Do("ROLE"); err != nil {
		return "", err
	} else {
		values, err := redigo.Values(reply, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		if len(values) == 0 {
			return "", errors.Errorf("invalid response = %v", reply)
		}
		role, err := redigo.String(values[0], nil)
		if err != nil {
			return "", errors.Errorf("invalid response[0] = %v", values[0])
		}
		return strings.ToUpper(role), nil
	}
}

var ErrClosedPool = errors.New("use of closed redis pool")

type Pool struct {
	mu sync.Mutex

	auth string
	pool map[string]*list.List

	timeout time.Duration

	exit struct {
		C chan struct{}
	}

	closed bool
}

func NewPool(auth string, timeout time.Duration) *Pool {
	p := &Pool{
		auth: auth, timeout: timeout,
		pool: make(map[string]*list.List),
	}
	p.exit.C = make(chan struct{})

	if timeout != 0 {
		go func() {
			var ticker = time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-p.exit.C:
					return
				case <-ticker.C:
					p.Cleanup()
				}
			}
		}()
	}

	return p
}

func (p *Pool) isRecyclable(c *Client) bool {
	if c.conn.Err() != nil {
		return false
	}
	return p.timeout == 0 || time.Since(c.LastUse) < p.timeout
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.exit.C)

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			c.Close()
		}
		delete(p.pool, addr)
	}
	return nil
}

func (p *Pool) Cleanup() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedPool
	}

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if p.isRecyclable(c) {
				list.PushBack(c)
			} else {
				c.Close()
			}
		}
		if list.Len() == 0 {
			delete(p.pool, addr)
		}
	}
	return nil
}

func (p *Pool) GetClient(addr string) (*Client, error) {
	c, err := p.getClientFromCache(addr)
	if err != nil || c != nil {
		return c, err
	}
	return NewClient(addr, p.auth, p.timeout)
}

func (p *Pool) getClientFromCache(addr string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosedPool
	}
	if list := p.pool[addr]; list != nil {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if p.isRecyclable(c) {
				return c, nil
			} else {
				c.Close()
			}
		}
	}
	return nil, nil
}

func (p *Pool) PutClient(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.isRecyclable(c) {
		c.Close()
	} else {
		cache := p.pool[c.Addr]
		if cache == nil {
			cache = list.New()
			p.pool[c.Addr] = cache
		}
		cache.PushFront(c)
	}
}

func (p *Pool) Info(addr string, gid int) (map[string]string, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.Info(gid)
}

func (p *Pool) InfoFull(addr string, gid int) (map[string]string, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.InfoFull(gid)
}

type InfoCache struct {
	mu sync.Mutex

	Auth      string
	data      map[string]map[string]string
	slaveSync map[string]map[string]int

	nodeInfo map[string]*NodeInfo
	Timeout  time.Duration
	pool     *Pool
}

func NewInfoCache(auth string, timeout time.Duration, pool *Pool) *InfoCache {
	return &InfoCache{
		mu:       sync.Mutex{},
		Auth:     auth,
		Timeout:  timeout,
		pool:     pool,
		nodeInfo: make(map[string]*NodeInfo),
	}
}

func (s *InfoCache) load(addr string, gid int) map[string]string {
	longaddr := utils.ServerGroupKey(addr, gid)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data != nil {
		return s.data[longaddr]
	}
	return nil
}

func (s *InfoCache) loadNodeInfo(addr string, gid int) (*NodeInfo, bool) {
	longaddr := utils.ServerGroupKey(addr, gid)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodeInfo[longaddr]; ok {
		return s.nodeInfo[longaddr], true
	}
	return nil, false
}

func (s *InfoCache) storeNodeInfo(addr string, gid int, nf *NodeInfo) {
	longaddr := utils.ServerGroupKey(addr, gid)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeInfo[longaddr] = nf
}

func (s *InfoCache) store(addr string, gid int, info map[string]string) map[string]string {
	longaddr := utils.ServerGroupKey(addr, gid)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]map[string]string)
	}
	if info != nil {
		s.data[longaddr] = info
	} else if s.data[longaddr] == nil {
		s.data[longaddr] = make(map[string]string)
	}
	return s.data[longaddr]
}

func (s *InfoCache) Get(addr string, gid int, force bool) (info map[string]string) {
	if !force {
		info = s.load(addr, gid)
		if info != nil {
			return info
		}
	}
	var err error
	if info, err = s.getInfo(addr, gid); err != nil {
		log.Warnf("get info fail, addr : %s, err : %s", addr, err.Error())
	}
	return s.store(addr, gid, info)
}

func (s *InfoCache) GetProcessId(addr string) string {
	return s.Get(addr, 0, false)["process_id"]
}

func (s *InfoCache) getInfo(addr string, gid int) (map[string]string, error) {
	if s.pool == nil {
		c, err := NewClient(addr, s.Auth, s.Timeout)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.Info(gid)
	} else {
		return s.pool.Info(addr, gid)
	}
}
