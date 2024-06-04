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

package resp

import (
	"fmt"
	"strings"
)

const (
	INFO    string = "INFO"
	COMMAND string = "COMMAND"
	PING    string = "PING"
	PONG    string = "PONG"
	ECHO    string = "ECHO"
	TYPE    string = "TYPE"

	AUTH     string = "AUTH"
	SHUTDOWN string = "SHUTDOWN"

	PKSETEXAT string = "PKSETEXAT"

	SET         string = "SET"
	SETEX       string = "SETEX"
	PSETEX      string = "PSETEX"
	SETNX       string = "SETNX"
	MSET        string = "MSET"
	DEL         string = "DEL"
	GET         string = "GET"
	GETSET      string = "GETSET"
	MGET        string = "MGET"
	INCR        string = "INCR"
	INCRBY      string = "INCRBY"
	INCRBYFLOAT string = "INCRBYFLOAT"
	DECR        string = "DECR"
	DECRBY      string = "DECRBY"
	EXISTS      string = "EXISTS"
	EXPIRE      string = "EXPIRE"
	EXPIREAT    string = "EXPIREAT"
	PEXPIRE     string = "PEXPIRE"
	PEXPIREAT   string = "PEXPIREAT"
	TTL         string = "TTL"
	PTTL        string = "PTTL"
	PERSIST     string = "PERSIST"

	UNLINK string = "UNLINK"
	SELECT string = "SELECT"

	KDEL      string = "KDEL"
	KTTL      string = "KTTL"
	KEXISTS   string = "KEXISTS"
	KEXPIRE   string = "KEXPIRE"
	KPERSIST  string = "KPERSIST"
	KEXPIREAT string = "KEXPIREAT"

	APPEND   string = "APPEND"
	GETRANGE string = "GETRANGE"
	SETRANGE string = "SETRANGE"
	STRLEN   string = "STRLEN"

	BITCOUNT string = "BITCOUNT"
	BITPOS   string = "BITPOS"
	GETBIT   string = "GETBIT"
	SETBIT   string = "SETBIT"

	HSET    string = "HSET"
	HMSET   string = "HMSET"
	HGET    string = "HGET"
	HMGET   string = "HMGET"
	HEXISTS string = "HEXISTS"
	HLEN    string = "HLEN"
	HKEYS   string = "HKEYS"
	HVALS   string = "HVALS"
	HDEL    string = "HDEL"
	HINCRBY string = "HINCRBY"
	HGETALL string = "HGETALL"
	HSCAN   string = "HSCAN"

	HCLEAR     string = "HCLEAR"
	HEXPIRE    string = "HEXPIRE"
	HEXPIREAT  string = "HEXPIREAT"
	HTTL       string = "HTTL"
	HPERSIST   string = "HPERSIST"
	HKEYEXISTS string = "HKEYEXISTS"

	SADD        string = "SADD"
	SREM        string = "SREM"
	SPOP        string = "SPOP"
	SCARD       string = "SCARD"
	SISMEMBER   string = "SISMEMBER"
	SMEMBERS    string = "SMEMBERS"
	SSCAN       string = "SSCAN"
	SRANDMEMBER string = "SRANDMEMBER"

	SCLEAR     string = "SCLEAR"
	SEXPIRE    string = "SEXPIRE"
	SEXPIREAT  string = "SEXPIREAT"
	STTL       string = "STTL"
	SPERSIST   string = "SPERSIST"
	SKEYEXISTS string = "SKEYEXISTS"

	ZADD             string = "ZADD"
	ZSCORE           string = "ZSCORE"
	ZCARD            string = "ZCARD"
	ZCOUNT           string = "ZCOUNT"
	ZINCRBY          string = "ZINCRBY"
	ZRANGE           string = "ZRANGE"
	ZRANGEBYSCORE    string = "ZRANGEBYSCORE"
	ZREVRANGEBYSCORE string = "ZREVRANGEBYSCORE"
	ZRANK            string = "ZRANK"
	ZREM             string = "ZREM"
	ZREMRANGEBYRANK  string = "ZREMRANGEBYRANK"
	ZREMRANGEBYSCORE string = "ZREMRANGEBYSCORE"
	ZREVRANGE        string = "ZREVRANGE"
	ZREVRANK         string = "ZREVRANK"
	ZREMRANGEBYLEX   string = "ZREMRANGEBYLEX"
	ZLEXCOUNT        string = "ZLEXCOUNT"
	ZSCAN            string = "ZSCAN"

	ZCLEAR      string = "ZCLEAR"
	ZEXPIRE     string = "ZEXPIRE"
	ZEXPIREAT   string = "ZEXPIREAT"
	ZTTL        string = "ZTTL"
	ZPERSIST    string = "ZPERSIST"
	ZKEYEXISTS  string = "ZKEYEXISTS"
	ZRANGEBYLEX string = "ZRANGEBYLEX"

	LPUSH   string = "LPUSH"
	RPUSH   string = "RPUSH"
	LPOP    string = "LPOP"
	RPOP    string = "RPOP"
	LLEN    string = "LLEN"
	LINDEX  string = "LINDEX"
	LRANGE  string = "LRANGE"
	LREM    string = "LREM"
	LINSERT string = "LINSERT"
	LSET    string = "LSET"
	LTRIM   string = "LTRIM"
	LPUSHX  string = "LPUSHX"
	RPUSHX  string = "RPUSHX"

	LCLEAR     string = "LCLEAR"
	LMCLEAR    string = "LMCLEAR"
	LEXPIRE    string = "LEXPIRE"
	LEXPIREAT  string = "LEXPIREAT"
	LTTL       string = "LTTL"
	LPERSIST   string = "LPERSIST"
	LKEYEXISTS string = "LKEYEXISTS"
	LTRIMBACK  string = "LTRIM_BACK"
	LTRIMFRONT string = "LTRIM_FRONT"

	WATCH   string = "WATCH"
	UNWATCH string = "UNWATCH"
	MULTI   string = "MULTI"
	EXEC    string = "EXEC"
	PREPARE string = "PREPARE"
	DISCARD string = "DISCARD"

	EVAL    string = "EVAL"
	SCRIPT  string = "SCRIPT"
	EVALSHA string = "EVALSHA"

	GEOADD            string = "GEOADD"
	GEODIST           string = "GEODIST"
	GEOPOS            string = "GEOPOS"
	GEOHASH           string = "GEOHASH"
	GEORADIUS         string = "GEORADIUS"
	GEORADIUSBYMEMBER string = "GEORADIUSBYMEMBER"
)

type CommandFunc func(c *Session) error

type Cmd struct {
	Name    string
	Handler func(c *Session) error
}

var regCmds = map[string]CommandFunc{}

func Register(name string, f CommandFunc) {
	if _, ok := regCmds[strings.ToLower(name)]; ok {
		panic(fmt.Sprintf("%s has been registered", name))
	}
	regCmds[name] = f
}
