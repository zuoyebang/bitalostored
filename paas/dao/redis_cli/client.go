// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis_cli

import (
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"reflect"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	redigo "github.com/gomodule/redigo/redis"
)

type Client struct {
	conn redigo.Conn
	Addr string
	Auth string

	Database int

	LastUse time.Time
	Timeout time.Duration
}

type ClusterMemberShip struct {
	Info Membership `json:"info"`
}

type Membership struct {
	ConfigChangeID uint              `json:"config_changeid"`
	Nodes          map[uint]string   `json:"nodes"`
	Observers      map[uint]string   `json:"observers"`
	Witnesses      map[uint]string   `json:"witnesses"`
	Removed        map[uint]struct{} `json:"removed"`
}

type StoredConnConf struct {
	HostPort     string        `toml:"host_port" json:"host_port,omitempty"`
	MaxIdle      int           `toml:"max_idle" json:"max_idle"`
	MaxActive    int           `toml:"max_active" json:"max_active"`
	IdleTimeout  time.Duration `toml:"idle_timeout" json:"idle_timeout"`
	ConnLifeTime time.Duration `toml:"conn_lifetime" json:"conn_lifetime"`
	Password     string        `toml:"password" json:"password"`
	DataBase     int           `toml:"database" json:"database"`
	ConnTimeout  time.Duration `toml:"conn_timeout" json:"conn_timeout"`
	ReadTimeout  time.Duration `toml:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `toml:"write_timeout" json:"write_timeout"`
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

func NewClient(addr string, auth string, timeout time.Duration) (*Client, error) {
	c, err := redigo.Dial("tcp", addr, []redigo.DialOption{
		redigo.DialConnectTimeout(math2.MinDuration(time.Second, timeout)),
		redigo.DialPassword(""),
		redigo.DialReadTimeout(timeout),
		redigo.DialWriteTimeout(timeout),
	}...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: c, Addr: addr, Auth: auth,
		LastUse: time.Now(), Timeout: timeout,
	}, nil
}

func (c *Client) Close() {
	if c != nil && c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) Getclustermembership() (*ClusterMemberShip, error) {
	text, err := redigo.String(c.Do("getclustermembership"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	var infos *ClusterMemberShip
	err = jsoniter.UnmarshalFromString(text, &infos)
	if err != nil {
		return nil, err
	}
	return infos, nil
}

func (c *Client) MergeInfoV67(groupId uint64) (map[string]string, error) {
	m1, err := c.Info()
	if err != nil {
		return nil, err
	}
	m2, err := c.ClusterInfo(groupId)
	if err != nil {
		return m1, nil
	}
	for k, v := range m2 {
		m1[k] = v
	}
	return m1, nil
}

func (c *Client) Info() (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO"))
	//log.Debug("Do Info：", text)

	if err != nil {
		return nil, err
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

func (c *Client) ClusterInfo(groupId uint64) (map[string]string, error) {
	text, err := redigo.String(c.Do("clusterinfo", groupId))

	if err != nil {
		return nil, err
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

func (c *Client) Shutdown() error {
	_, err := redigo.String(c.Do("SHUTDOWN"))
	//log.Debug("Do Info：", text)

	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddWitness(witnessRaftAddr string, nodeId, gid uint) error {
	_, err := redigo.String(c.Do("addwitness", witnessRaftAddr, nodeId, gid))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Reraft(token string, port uint) error {
	_, err := redigo.String(c.Do("reraft", token, port))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Deraft(token string) error {
	_, err := redigo.String(c.Do("deraft", token))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddOb(addr string, nodeId, gid uint) error {
	_, err := redigo.String(c.Do("addobserver", addr, nodeId, gid))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddObV6(addr string, nodeId, gid uint) error {
	_, err := redigo.String(c.Do("addobserver", addr, nodeId, gid))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddSlave(addr string, nodeId, gid uint) error {
	_, err := redigo.String(c.Do("add", addr, nodeId, gid))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Derafttoobserver(token string, newPort, masterNodeId uint, masterRaftAddr string) error {
	_, err := redigo.String(c.Do("derafttoobserver", token, newPort, masterRaftAddr, masterNodeId))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Derafttowitness(token string, newPort, masterNodeId uint, masterRaftAddr string) error {
	_, err := redigo.String(c.Do("derafttowitness", token, newPort, masterRaftAddr, masterNodeId))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Smembers(key string) ([]uint, error) {
	r, err := redigo.ByteSlices(c.Do("smembers", key))
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make([]uint, 0, len(r))
	for _, n := range r {
		ns := string(n)
		in, _ := strconv.Atoi(ns)
		ret = append(ret, uint(in))
	}
	return ret, nil
}

func (c *Client) Llen(key string) (int, error) {
	return redigo.Int(c.Do("llen", key))
}

func (c *Client) Scard(key string) (int, error) {
	return redigo.Int(c.Do("scard", key))
}

func (c *Client) SRem(key string, args ...interface{}) (int64, error) {
	packs := make([]interface{}, 0)
	packs = append(packs, key)
	for _, item := range args {
		packs = append(packs, item)
	}
	r, err := redigo.Int64(c.Do("srem", packs...))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) LPush(key string, args ...interface{}) (int, error) {
	packs := make([]interface{}, 0)
	packs = append(packs, key)
	for _, item := range args {
		packs = append(packs, item)
	}
	r, err := redigo.Int(c.Do("lpush", packs...))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) RPush(key string, args ...interface{}) (int, error) {
	packs := make([]interface{}, 0)
	packs = append(packs, key)
	for _, item := range args {
		packs = append(packs, item)
	}
	r, err := redigo.Int(c.Do("rpush", packs...))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) LRange(key string, start, end int) ([][]byte, error) {
	if res, err := redigo.ByteSlices(c.Do("LRANGE", key, start, end)); err == redigo.ErrNil {
		return nil, nil
	} else {
		return res, err
	}
}

func (c *Client) RPop(key string) ([]byte, error) {
	r, err := redigo.Bytes(c.Do("rpop", key))
	return r, err
}

func (c *Client) SAdd(key string, args ...interface{}) (int, error) {
	packs := make([]interface{}, 0)
	packs = append(packs, key)
	for _, item := range args {
		packs = append(packs, item)
	}
	r, err := redigo.Int(c.Do("sadd", packs...))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) HSet(key string, field string, value []byte) error {
	_, err := c.Do("HSET", key, field, value)
	return err
}

func (c *Client) HGet(key string, field string) ([]byte, error) {
	if res, err := redigo.Bytes(c.Do("HGET", key, field)); err == redigo.ErrNil {
		return nil, nil
	} else {
		return res, err
	}
}

func (c *Client) Expire(key string, ts int) error {
	_, err := c.Do("expire", key, ts)
	return err
}

func (c *Client) HMSet(key string, fvmap map[string]interface{}) error {
	args := PackArgs(key, fvmap)
	_, err := c.Do("HMSET", args...)
	return err
}

func (c *Client) HMGet(key string, fields ...string) ([][]byte, error) {
	if res, err := redigo.ByteSlices(c.Do("HMGET", redigo.Args{}.Add(key).AddFlat(fields)...)); err == redigo.ErrNil {
		return nil, nil
	} else {
		return res, err
	}
}

func (c *Client) HGetall(key string) ([][]byte, error) {
	if res, err := redigo.ByteSlices(c.Do("HGETALL", key)); err == redigo.ErrNil {
		return nil, nil
	} else {
		return res, err
	}
}

func (c *Client) Del(keys ...interface{}) (int64, error) {
	args := PackArgs(keys)
	return redigo.Int64(c.Do("DEL", args...))
}

func (c *Client) DirectDo(command string) (string, error) {
	return redigo.String(c.Do(command))
}

func PackArgs(items ...interface{}) (args []interface{}) {
	for _, item := range items {
		v := reflect.ValueOf(item)
		switch v.Kind() {
		case reflect.Slice:
			if v.IsNil() {
				continue
			}
			for i := 0; i < v.Len(); i++ {
				args = append(args, v.Index(i).Interface())
			}
		case reflect.Map:
			if v.IsNil() {
				continue
			}
			for _, key := range v.MapKeys() {
				args = append(args, key.Interface(), v.MapIndex(key).Interface())
			}
		default:
			args = append(args, v.Interface())
		}
	}
	return args
}
