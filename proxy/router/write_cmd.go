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

package router

import (
	"github.com/zuoyebang/bitalostored/proxy/resp"
)

var writeCommand = map[string]bool{
	resp.DEL:       true,
	resp.EXPIRE:    true,
	resp.PERSIST:   true,
	resp.EXPIREAT:  true,
	resp.PEXPIRE:   true,
	resp.PEXPIREAT: true,

	"SET":          true,
	"SETNX":        true,
	"SETEX":        true,
	"PSETEX":       true,
	"GET":          false,
	"GETSET":       true,
	"STRLEN":       false,
	"APPEND":       true,
	"SETRANGE":     true,
	"GETRANGE":     false,
	"INCR":         true,
	"INCRBY":       true,
	"INCRBYFLOAT":  true,
	"DECR":         true,
	"DECRBY":       true,
	"MSET":         true,
	"MSETNX":       true,
	"MGET":         false,
	resp.SETBIT:    true,
	resp.KDEL:      true,
	resp.KEXPIRE:   true,
	resp.KEXPIREAT: true,
	resp.KPERSIST:  true,
	resp.KTTL:      false,
	resp.KEXISTS:   false,
	resp.BITCOUNT:  false,
	resp.BITPOS:    false,
	resp.GETBIT:    false,

	"LPUSH":         true,
	"LPUSHX":        true,
	"RPUSH":         true,
	"RPUSHX":        true,
	"LPOP":          true,
	"RPOP":          true,
	"RPOPLPUSH":     true,
	"LREM":          true,
	"LLEN":          false,
	"LINDEX":        false,
	"LINSERT":       true,
	"LSET":          true,
	"LRANGE":        false,
	"LTRIM":         true,
	"BLPOP":         true,
	"BRPOP":         true,
	"BRPOPLPUSH":    true,
	resp.LCLEAR:     true,
	resp.LMCLEAR:    true,
	resp.LEXPIRE:    true,
	resp.LEXPIREAT:  true,
	resp.LPERSIST:   true,
	resp.LTRIMFRONT: true,
	resp.LTRIMBACK:  true,
	resp.LTTL:       false,
	resp.LKEYEXISTS: false,

	"HSET":          true,
	"HSETNX":        true,
	"HGET":          false,
	"HEXISTS":       false,
	"HDEL":          true,
	"HLEN":          false,
	"HSTRLEN":       false,
	"HINCRBY":       true,
	"HINCRBYFLOAT":  true,
	"HMSET":         true,
	"HMGET":         false,
	"HKEYS":         false,
	"HVALS":         false,
	"HGETALL":       false,
	"HSCAN":         true,
	resp.HCLEAR:     true,
	resp.HEXPIRE:    true,
	resp.HEXPIREAT:  true,
	resp.HPERSIST:   true,
	resp.HKEYEXISTS: false,
	resp.HTTL:       false,

	"SADD":          true,
	"SISMEMBER":     false,
	"SPOP":          true,
	"SRANDMEMBER":   false,
	"SREM":          true,
	"SMOVE":         true,
	"SCARD":         false,
	"SMEMBERS":      false,
	"SSCAN":         true,
	"SINTER":        false,
	"SINTERSTORE":   true,
	"SUNION":        false,
	"SUNIONSTORE":   true,
	"SDIFF":         false,
	"SDIFFSTORE":    true,
	resp.SCLEAR:     true,
	resp.SEXPIRE:    true,
	resp.SEXPIREAT:  true,
	resp.SPERSIST:   true,
	resp.STTL:       false,
	resp.SKEYEXISTS: false,

	"ZADD":             true,
	"ZSCORE":           false,
	"ZINCRBY":          true,
	"ZCARD":            false,
	"ZCOUNT":           false,
	"ZRANGE":           false,
	"ZREVRANGE":        false,
	"ZRANGEBYSCORE":    false,
	"ZREVRANGEBYSCORE": false,
	"ZRANK":            false,
	"ZREVRANK":         false,
	"ZREM":             true,
	"ZREMRANGEBYRANK":  true,
	"ZREMRANGEBYSCORE": true,
	"ZRANGEBYLEX":      false,
	"ZLEXCOUNT":        false,
	"ZREMRANGEBYLEX":   true,
	"ZSCAN":            true,
	"ZUNIONSTORE":      true,
	"ZINTERSTORE":      true,
	resp.ZCLEAR:        true,
	resp.ZEXPIRE:       true,
	resp.ZEXPIREAT:     true,
	resp.ZPERSIST:      true,
	resp.ZKEYEXISTS:    false,
	resp.ZTTL:          false,

	resp.SCRIPT:  true,
	resp.EVAL:    true,
	resp.EVALSHA: true,

	resp.GEOADD:            true,
	resp.GEODIST:           false,
	resp.GEOPOS:            false,
	resp.GEOHASH:           false,
	resp.GEORADIUS:         false,
	resp.GEORADIUSBYMEMBER: false,
}

func IsWriteCmd(commandName string) bool {
	if isWrite, ok := writeCommand[commandName]; ok {
		return isWrite
	}
	return false
}
