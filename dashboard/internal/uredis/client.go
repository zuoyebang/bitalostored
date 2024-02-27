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
)

type Client struct {
	conn redigo.Conn
	Addr string
	Auth string

	Database int

	LastUse time.Time
	Timeout time.Duration
}

type NodeInfo struct {
	NodeStatus     bool
	CurrentNodeId  string
	CurrentAddress string
	StartModel     string
	Role           string
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
		redigo.DialReadTimeout(math2.MinDuration(time.Second, timeout)),
		redigo.DialWriteTimeout(math2.MinDuration(time.Second, timeout)),
	}...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Client{
		conn: c, Addr: addr, Auth: auth,
		LastUse: time.Now(), Timeout: timeout,
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

func (c *Client) Select(database int) error {
	if c.Database == database {
		return nil
	}
	_, err := c.Do("SELECT", database)
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	c.Database = database
	return nil
}

func (c *Client) Shutdown() error {
	_, err := c.Do("SHUTDOWN")
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) LogCompact() error {
	_, err := c.Do("setex", "test@#$!stored_logcompact", 1, "1")
	if err != nil {
		return err
	}
	_, err = c.Do("logcompact")
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Info() (map[string]string, error) {
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

func (c *Client) ClusterInfo() (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO", "clusterinfo"))

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

func (c *Client) InfoFull() (map[string]string, error) {
	if info, err := c.Info(); err != nil {
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

func (c *Client) DebugInfoFull() (map[string]string, error) {
	if info, err := c.DebugInfo(); err != nil {
		return nil, errors.Trace(err)
	} else {
		return info, nil
	}
}

func (c *Client) PromoteMaster() error {
	if info, err := c.Info(); err != nil {
		return errors.Trace(err)
	} else {
		currentStatus := info["status"]
		currentRaftNodeId := info["current_node_id"]
		if currentStatus == "true" && len(currentRaftNodeId) > 0 {
			if ok, err := redigo.String(c.Do("transfer", currentRaftNodeId)); err == nil && strings.ToLower(ok) == "ok" {
				return nil
			} else if err != nil {
				return err
			} else {
				return errors.New(fmt.Sprintf("do promote server err, master : %s repley : %s", c.Addr, ok))
			}
		}
	}
	return nil
}

func (c *Client) SetMaster(master string) error {
	host, port, err := net.SplitHostPort(master)
	log.Infof("SplitHostPort host:%s,port:%s,err", host, port, err)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := c.Do("CONFIG", "SET", "masterauth", c.Auth); err != nil {
		log.Infof("CONFIG host:%s,port:%s,err", host, port, err)
		return errors.Trace(err)
	}

	if _, err := c.Do("SLAVEOF", host, port); err != nil {
		log.Infof("SLAVEOF host:%s,port:%s,err", host, port, err)
		return errors.Trace(err)
	}

	return nil
}

func (c *Client) MigrateCallback(callback_url string) error {
	if _, err := c.Do("MIGRATECALLBACK", callback_url); err != nil {
		return errors.Trace(err)
	}
	return nil
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

func (c *Client) AddObserver(raftAddress string, nodeId int) error {
	if _, err := c.Do("addobserver", raftAddress, nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddWitness(raftAddress string, nodeId int) error {
	if _, err := c.Do("addwitness", raftAddress, nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) RemoveRaftNode(nodeId int) error {
	if _, err := c.Do("remove", nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) GetClusterMemberShip() (*MembershipV2, error) {
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

func (c *Client) DeRaft(token string) error {
	var err error
	if _, err = c.Do("deraft", token); err != nil {
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

func (c *Client) GetNodeHostInfo() (string, error) {
	if data, err := redigo.String(c.Do("getnodehostinfo")); err != nil {
		return "", errors.Trace(err)
	} else {
		return data, nil
	}
}

func (c *Client) AddToSlave(raftAddress string, nodeId int) error {
	if _, err := c.Do("add", raftAddress, nodeId); err != nil {
		return errors.Trace(err)
	}
	return nil
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

func (p *Pool) Info(addr string) (map[string]string, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.Info()
}

func (p *Pool) ClusterInfo(addr string) (map[string]string, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.ClusterInfo()
}

func (p *Pool) InfoFull(addr string) (map[string]string, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.InfoFull()
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

func (s *InfoCache) load(addr string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data != nil {
		return s.data[addr]
	}
	return nil
}

func (s *InfoCache) loadNodeInfo(addr string) (*NodeInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodeInfo[addr]; ok {
		return s.nodeInfo[addr], true
	}
	return nil, false
}

func (s *InfoCache) storeNodeInfo(addr string, nf *NodeInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeInfo[addr] = nf
}

func (s *InfoCache) store(addr string, info map[string]string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]map[string]string)
	}
	if info != nil {
		s.data[addr] = info
	} else if s.data[addr] == nil {
		s.data[addr] = make(map[string]string)
	}
	return s.data[addr]
}

func (s *InfoCache) Get(addr string, force bool) (info map[string]string) {
	if !force {
		info = s.load(addr)
		if info != nil {
			return info
		}
	}
	var err error
	if info, err = s.getInfo(addr); err != nil {
		log.Warnf("get info fail, addr : %s, err : %s", addr, err.Error())
	}
	return s.store(addr, info)
}

func (s *InfoCache) GetProcessId(addr string) string {
	return s.Get(addr, false)["process_id"]
}

func (s *InfoCache) getInfo(addr string) (map[string]string, error) {
	if s.pool == nil {
		c, err := NewClient(addr, s.Auth, s.Timeout)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.Info()
	} else {
		return s.pool.Info(addr)
	}
}
