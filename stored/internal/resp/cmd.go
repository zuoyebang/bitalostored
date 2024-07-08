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

const (
	PING     string = "ping"
	PONG     string = "pong"
	ECHO     string = "echo"
	TYPE     string = "type"
	CONFIG   string = "config"
	INFO     string = "info"
	TIME     string = "time"
	SHUTDOWN string = "shutdown"

	DEL         string = "del"
	TTL         string = "ttl"
	PTTL        string = "pttl"
	EXISTS      string = "exists"
	PERSIST     string = "persist"
	EXPIRE      string = "expire"
	EXPIREAT    string = "expireat"
	PEXPIRE     string = "pexpire"
	PEXPIREAT   string = "pexpireat"
	SCAN        string = "scan"
	SET         string = "set"
	SETEX       string = "setex"
	PSETEX      string = "psetex"
	SETNX       string = "setnx"
	MSET        string = "mset"
	GET         string = "get"
	GETSET      string = "getset"
	MGET        string = "mget"
	INCR        string = "incr"
	INCRBY      string = "incrby"
	INCRBYFLOAT string = "incrbyfloat"
	DECR        string = "decr"
	DECRBY      string = "decrby"

	KDEL      string = "kdel"
	KTTL      string = "kttl"
	KEXISTS   string = "kexists"
	KEXPIRE   string = "kexpire"
	KPERSIST  string = "kpersist"
	KEXPIREAT string = "kexpireat"

	APPEND   string = "append"
	GETRANGE string = "getrange"
	SETRANGE string = "setrange"
	STRLEN   string = "strlen"

	BITCOUNT string = "bitcount"
	BITPOS   string = "bitpos"
	GETBIT   string = "getbit"
	SETBIT   string = "setbit"

	HSET    string = "hset"
	HMSET   string = "hmset"
	HGET    string = "hget"
	HMGET   string = "hmget"
	HEXISTS string = "hexists"
	HLEN    string = "hlen"
	HKEYS   string = "hkeys"
	HVALS   string = "hvals"
	HDEL    string = "hdel"
	HINCRBY string = "hincrby"
	HGETALL string = "hgetall"
	HSCAN   string = "hscan"

	HCLEAR     string = "hclear"
	HEXPIRE    string = "hexpire"
	HEXPIREAT  string = "hexpireat"
	HTTL       string = "httl"
	HPERSIST   string = "hpersist"
	HKEYEXISTS string = "hkeyexists"

	SADD        string = "sadd"
	SREM        string = "srem"
	SPOP        string = "spop"
	SCARD       string = "scard"
	SISMEMBER   string = "sismember"
	SMEMBERS    string = "smembers"
	SRANDMEMBER string = "srandmember"
	SSCAN       string = "sscan"

	SCLEAR     string = "sclear"
	SEXPIRE    string = "sexpire"
	SEXPIREAT  string = "sexpireat"
	STTL       string = "sttl"
	SPERSIST   string = "spersist"
	SKEYEXISTS string = "skeyexists"

	ZADD             string = "zadd"
	ZSCORE           string = "zscore"
	ZCARD            string = "zcard"
	ZCOUNT           string = "zcount"
	ZINCRBY          string = "zincrby"
	ZRANGE           string = "zrange"
	ZRANGEBYSCORE    string = "zrangebyscore"
	ZREVRANGEBYSCORE string = "zrevrangebyscore"
	ZRANK            string = "zrank"
	ZREM             string = "zrem"
	ZREMRANGEBYRANK  string = "zremrangebyrank"
	ZREMRANGEBYSCORE string = "zremrangebyscore"
	ZREVRANGE        string = "zrevrange"
	ZREVRANK         string = "zrevrank"
	ZREMRANGEBYLEX   string = "zremrangebylex"
	ZLEXCOUNT        string = "zlexcount"
	ZSCAN            string = "zscan"

	ZCLEAR      string = "zclear"
	ZEXPIRE     string = "zexpire"
	ZEXPIREAT   string = "zexpireat"
	ZTTL        string = "zttl"
	ZPERSIST    string = "zpersist"
	ZKEYEXISTS  string = "zkeyexists"
	ZRANGEBYLEX string = "zrangebylex"

	LPUSH   string = "lpush"
	RPUSH   string = "rpush"
	LPOP    string = "lpop"
	RPOP    string = "rpop"
	LLEN    string = "llen"
	LINDEX  string = "lindex"
	LRANGE  string = "lrange"
	LREM    string = "lrem"
	LINSERT string = "linsert"
	LSET    string = "lset"
	LTRIM   string = "ltrim"
	LPUSHX  string = "lpushx"
	RPUSHX  string = "rpushx"

	LCLEAR     string = "lclear"
	LMCLEAR    string = "lmclear"
	LEXPIRE    string = "lexpire"
	LEXPIREAT  string = "lexpireat"
	LTTL       string = "lttl"
	LPERSIST   string = "lpersist"
	LKEYEXISTS string = "lkeyexists"
	LTRIMBACK  string = "ltrim_back"
	LTRIMFRONT string = "ltrim_front"

	XHSCAN string = "xhscan"
	XSSCAN string = "xsscan"
	XZSCAN string = "xzscan"

	GEOADD            string = "geoadd"
	GEODIST           string = "geodist"
	GEOPOS            string = "geopos"
	GEOHASH           string = "geohash"
	GEORADIUS         string = "georadius"
	GEORADIUSBYMEMBER string = "georadiusbymember"

	EVAL         string = "eval"
	EVALSHA      string = "evalsha"
	SCRIPTLOAD   string = "scriptload"
	SCRIPTFLUSH  string = "scriptflush"
	SCRIPTEXISTS string = "scriptexists"
	SCRIPTLEN    string = "scriptlen"

	WATCH   string = "watch"
	UNWATCH string = "unwatch"
	MULTI   string = "multi"
	PREPARE string = "prepare"
	EXEC    string = "exec"
	DISCARD string = "discard"
)

var commandToWrite = map[string]bool{
	PING: false,
	PONG: false,
	ECHO: false,
	TYPE: false,

	SCAN:   false,
	HSCAN:  false,
	XHSCAN: false,
	XSSCAN: false,
	SSCAN:  false,
	XZSCAN: false,
	ZSCAN:  false,

	DEL:       true,
	PERSIST:   true,
	EXPIRE:    true,
	EXPIREAT:  true,
	PEXPIRE:   true,
	PEXPIREAT: true,

	TTL:    false,
	PTTL:   false,
	EXISTS: false,

	HDEL:    true,
	HINCRBY: true,
	HMSET:   true,
	HSET:    true,

	HVALS:   false,
	HEXISTS: false,
	HGET:    false,
	HGETALL: false,
	HKEYS:   false,
	HLEN:    false,
	HMGET:   false,

	HCLEAR:    true,
	HEXPIRE:   true,
	HEXPIREAT: true,
	HPERSIST:  true,

	HKEYEXISTS: false,
	HTTL:       false,

	LREM:    true,
	LINSERT: true,
	LPUSHX:  true,
	RPUSHX:  true,
	LPOP:    true,
	LPUSH:   true,
	RPOP:    true,
	RPUSH:   true,
	LSET:    true,

	LINDEX: false,
	LLEN:   false,
	LRANGE: false,

	LCLEAR:     true,
	LMCLEAR:    true,
	LEXPIRE:    true,
	LEXPIREAT:  true,
	LPERSIST:   true,
	LTRIMFRONT: true,
	LTRIMBACK:  true,
	LTRIM:      true,

	LTTL:       false,
	LKEYEXISTS: false,

	SET:         true,
	APPEND:      true,
	DECR:        true,
	DECRBY:      true,
	GETSET:      true,
	INCR:        true,
	INCRBY:      true,
	INCRBYFLOAT: true,
	MSET:        true,
	SETNX:       true,
	SETEX:       true,
	PSETEX:      true,
	SETRANGE:    true,
	SETBIT:      true,
	KDEL:        true,
	KEXPIRE:     true,
	KEXPIREAT:   true,
	KPERSIST:    true,

	KTTL:     false,
	GETRANGE: false,
	MGET:     false,
	STRLEN:   false,
	KEXISTS:  false,
	GET:      false,
	BITCOUNT: false,
	BITPOS:   false,
	GETBIT:   false,

	SADD:      true,
	SREM:      true,
	SCLEAR:    true,
	SEXPIRE:   true,
	SEXPIREAT: true,
	SPERSIST:  true,
	SPOP:      true,

	STTL:       false,
	SCARD:      false,
	SISMEMBER:  false,
	SMEMBERS:   false,
	SKEYEXISTS: false,

	ZADD:             true,
	ZINCRBY:          true,
	ZREM:             true,
	ZREMRANGEBYSCORE: true,
	ZREMRANGEBYRANK:  true,
	ZREMRANGEBYLEX:   true,

	ZRANGE:           false,
	ZREVRANGE:        false,
	ZRANGEBYLEX:      false,
	ZRANGEBYSCORE:    false,
	ZREVRANGEBYSCORE: false,
	ZRANK:            false,
	ZREVRANK:         false,
	ZSCORE:           false,
	ZLEXCOUNT:        false,
	ZCOUNT:           false,
	ZCARD:            false,

	ZCLEAR:     true,
	ZEXPIRE:    true,
	ZEXPIREAT:  true,
	ZPERSIST:   true,
	ZKEYEXISTS: false,
	ZTTL:       false,

	SCRIPTLOAD:   true,
	SCRIPTEXISTS: false,
	SCRIPTFLUSH:  true,

	GEOADD:            true,
	GEODIST:           false,
	GEOPOS:            false,
	GEOHASH:           false,
	GEORADIUS:         false,
	GEORADIUSBYMEMBER: false,

	WATCH:   false,
	UNWATCH: false,
	MULTI:   false,
	PREPARE: false,
	EXEC:    false,
	DISCARD: false,
}

func IsWriteCmd(cmd string) bool {
	if res, ok := commandToWrite[cmd]; ok {
		return res
	}
	return false
}

type Command struct {
	Raw  []byte
	Args [][]byte
}
