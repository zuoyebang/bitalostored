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

package engine

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/locker"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/task"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

var MigrateLuaScript = "redis.call(KEYS)"

const (
	MigrateStatusPrepare = 0
	MigrateStatusProcess = 1
	MigrateStatusFinish  = 2
	MigrateStatusError   = 3
)

type Migrate struct {
	Conn      *redis.Pool
	IsMigrate atomic.Int32 `json:"is_migrate"`

	fromHost          string
	toHost            string
	slotId            uint32
	migrateDelToSlave func(uint32, [][]byte) error
	keyLocker         *locker.ScopeLocker
	db                *bitsdb.BitsDB
	status            int64
	total             int64
	fails             int64
	beginTime         time.Time
	endTime           time.Time
}

func (m *Migrate) migrateDirectTTL(key []byte, ttl int64, conn redis.Conn, isHashTag bool) error {
	if ttl > -1 {
		if isHashTag {
			if _, err := conn.Do(resp.EVAL, MigrateLuaScript, 3, resp.PEXPIRE, key, ttl); err != nil {
				log.Errorf("migrate direct ttl key:%s err:%s ttl:%d", string(key), err, ttl)
				return err
			}
		} else {
			if _, err := conn.Do(resp.PEXPIRE, key, ttl); err != nil {
				log.Errorf("migrate direct ttl key:%s err:%s ttl:%d", string(key), err, ttl)
				return err
			}
		}
	}

	return nil
}

func (m *Migrate) migrateTTL(key []byte, khash uint32, conn redis.Conn, isHashTag bool) error {
	var ttl int64
	var err error
	if ttl, err = m.db.StringObj.PTTL(key, khash); err != nil {
		log.Warnf("migrate string pttl key:%s err:%s", string(key), err)
		return err
	}

	return m.migrateDirectTTL(key, ttl, conn, isHashTag)
}

func (m *Migrate) getKeyHash(key []byte) (uint32, bool) {
	var isHashTag bool
	khash := hash.Fnv32(key)
	slotId := khash % utils.TotalSlot
	if slotId != m.slotId {
		isHashTag = true
		khash = utils.GetHashTagFnv(key)
	}

	return khash, isHashTag
}

func (m *Migrate) migrateString(key []byte, conn redis.Conn) error {
	if m.slotId == uint32(btools.LuaScriptSlot) {
		val, closer := m.db.StringObj.GetLuaScript(key)
		defer func() {
			if closer != nil {
				closer()
			}
		}()
		if val == nil {
			return nil
		}
		if _, err := conn.Do(resp.SCRIPTLOAD, "load", val); err != nil {
			log.Errorf("migrate string lua script key:%s err:%s", string(key), err)
			return err
		}
		return nil
	}

	khash, isHashTag := m.getKeyHash(key)
	val, valCloser, ttl, err := m.db.StringObj.GetWithTTL(key, khash)
	defer func() {
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil {
		log.Errorf("migrate string get key:%s err:%s", string(key), err)
		return err
	} else if val == nil {
		return nil
	}

	if isHashTag {
		if _, err = conn.Do(resp.EVAL, MigrateLuaScript, 3, resp.SET, key, val); err != nil {
			log.Errorf("migrate string send key:%s err:%s", string(key), err)
			return err
		}
	} else if _, err = conn.Do(resp.SET, key, val); err != nil {
		log.Errorf("migrate string send key:%s err:%s", string(key), err)
		return err
	}

	if err := m.migrateDirectTTL(key, ttl, conn, isHashTag); err != nil {
		return err
	}
	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.KDEL), key}); err != nil {
		log.Errorf("migrate string sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.StringObj.Del(khash, key); err != nil {
		log.Warnf("migrate string del key:%s err:%s", string(key), err)
	}

	return nil
}

func (m *Migrate) migrateStringRetry(key []byte, conn redis.Conn) error {
	if m.slotId == uint32(btools.LuaScriptSlot) {
		val, closer := m.db.StringObj.GetLuaScript(key)
		defer func() {
			if closer != nil {
				closer()
			}
		}()
		if val == nil {
			return nil
		}
		if _, err := conn.Do(resp.SCRIPTLOAD, "load", val); err != nil {
			log.Errorf("migrateretry string lua script key:%s err:%s", string(key), err)
			return err
		}
		return nil
	}

	khash, isHashTag := m.getKeyHash(key)
	val, valCloser, ttl, err := m.db.StringObj.GetWithTTL(key, khash)
	defer func() {
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil {
		log.Errorf("migrateretry string get key:%s err:%s", string(key), err)
		return err
	} else if val == nil {
		return nil
	}

	var n int
	if isHashTag {
		if _, err = conn.Do(resp.EVAL, MigrateLuaScript, 3, resp.SET, key, val); err != nil {
			log.Errorf("migrateretry string send key:%s err:%s", string(key), err)
			return err
		}
	} else if n, err = redis.Int(conn.Do(resp.SETNX, key, val)); err != nil {
		log.Errorf("migrateretry string send key:%s err:%s", string(key), err)
		return err
	}

	if n == 1 {
		if err := m.migrateDirectTTL(key, ttl, conn, isHashTag); err != nil {
			return err
		}
	}

	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.KDEL), key}); err != nil {
		log.Errorf("migrateretry string sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.StringObj.Del(khash, key); err != nil {
		log.Warnf("migrateretry string del key:%s err:%s", string(key), err)
	}

	return nil
}

func (m *Migrate) migrateHash(key []byte, conn redis.Conn) error {
	khash, isHashTag := m.getKeyHash(key)
	list, closers, err := m.db.HashObj.HGetAll(key, khash)
	defer func() {
		if len(closers) > 0 {
			for _, closer := range closers {
				closer()
			}
		}
	}()
	if err != nil {
		log.Errorf("migrate hash hgetall key:%s err:%s", string(key), err)
		return err
	} else if len(list) == 0 {
		return nil
	}

	args := []interface{}{key}
	for i := 0; i < len(list); i++ {
		args = append(args, list[i].Field, list[i].Value)
		if i < len(list)-1 && i%10 != 0 {
			continue
		}

		if isHashTag {
			hashArgs := make([]interface{}, 0, 3+len(args))
			hashArgs = append(hashArgs, MigrateLuaScript, len(args)+1, resp.HMSET)
			for l := 0; l < len(args); l++ {
				hashArgs = append(hashArgs, args[l])
			}
			if _, err = conn.Do(resp.EVAL, hashArgs...); err != nil {
				log.Errorf("migrate hash send key:%s err:%s", string(key), err)
				return err
			}
		} else {
			if _, err = conn.Do(resp.HMSET, args...); err != nil {
				log.Errorf("migrate hash send key:%s err:%s", string(key), err)
				return err
			}
		}
		args = []interface{}{key}
	}

	if err := m.migrateTTL(key, khash, conn, isHashTag); err != nil {
		return err
	}
	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.HCLEAR), key}); err != nil {
		log.Errorf("migrate hash sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.HashObj.Del(khash, key); err != nil {
		log.Warnf("migrate hash del key:%s err:%s", string(key), err)
	}
	return nil
}

func (m *Migrate) migrateHashRetry(key []byte, conn redis.Conn) error {
	khash, isHashTag := m.getKeyHash(key)
	list, closers, err := m.db.HashObj.HGetAll(key, khash)
	defer func() {
		if len(closers) > 0 {
			for _, closer := range closers {
				closer()
			}
		}
	}()
	if err != nil {
		log.Errorf("migrateretry hash hgetall key:%s err:%s", string(key), err)
		return err
	} else if len(list) == 0 {
		return nil
	}

	if isHashTag {
		args := []interface{}{key}
		for i := 0; i < len(list); i++ {
			args = append(args, list[i].Field, list[i].Value)
			if i < len(list)-1 && i%10 != 0 {
				continue
			}

			hashArgs := make([]interface{}, 0, 3+len(args))
			hashArgs = append(hashArgs, MigrateLuaScript, len(args)+1, resp.HMSET)
			for l := 0; l < len(args); l++ {
				hashArgs = append(hashArgs, args[l])
			}
			if _, err = conn.Do(resp.EVAL, hashArgs...); err != nil {
				log.Errorf("migrateretry hash send key:%s err:%s", string(key), err)
				return err
			}
			args = []interface{}{key}
		}
	} else {
		for i := 0; i < len(list); i++ {
			res, e := conn.Do(resp.HGET, key, list[i].Field)
			if e == nil && res == nil {
				if _, err = conn.Do(resp.HSET, key, list[i].Field, list[i].Value); err != nil {
					log.Errorf("migrateretry hash send key:%s field:%s err:%s", key, list[i].Field, err)
					return err
				}
			}
		}
	}

	if err := m.migrateTTL(key, khash, conn, isHashTag); err != nil {
		return err
	}
	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.HCLEAR), key}); err != nil {
		log.Errorf("migrateretry hash sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.HashObj.Del(khash, key); err != nil {
		log.Warnf("migrateretry hash del key:%s err:%s", string(key), err)
	}
	return nil
}

func (m *Migrate) migrateList(key []byte, conn redis.Conn) error {
	var step int64 = 5000
	var start int64 = 0

	khash, isHashTag := m.getKeyHash(key)
	listSize, err := m.db.ListObj.LLen(key, khash)
	if err != nil {
		log.Errorf("migrate list llen key:%s err:%s", string(key), err)
		return err
	}
	if listSize == 0 {
		return nil
	}

	var stop int64
	var list [][]byte
	var currentLen int
	args := make([]interface{}, 0, 20)
	args = append(args, key)
	for {
		stop = start + step - 1
		list, err = m.db.ListObj.LRange(key, khash, start, stop)
		currentLen = len(list)
		if err != nil {
			log.Errorf("migrate list lrange key:%s err:%s", string(key), err)
			return err
		} else if currentLen == 0 {
			break
		}

		for i := 0; i < currentLen; i++ {
			args = append(args, list[i])
			if i < currentLen-1 && (i+1)%10 != 0 {
				continue
			}

			if isHashTag {
				hashArgs := make([]interface{}, 0, 3+len(args))
				hashArgs = append(hashArgs, MigrateLuaScript, len(args)+1, resp.RPUSH)
				for l := 0; l < len(args); l++ {
					hashArgs = append(hashArgs, args[l])
				}
				if _, err = conn.Do(resp.EVAL, hashArgs...); err != nil {
					log.Errorf("migrate list send key:%s err:%s", string(key), err)
					return err
				}
			} else {
				if _, err = conn.Do(resp.RPUSH, args...); err != nil {
					log.Errorf("migrate list send key:%s err:%s", string(key), err)
					return err
				}
			}
			args = args[0:1]
		}
		if int64(currentLen) < step {
			log.Infof("migrate list key:%s len:%d start:%d stop:%d fetched:%d", string(key), listSize, start, stop, currentLen)
			break
		}
		list = nil
		start = stop + 1
	}

	if err := m.migrateTTL(key, khash, conn, isHashTag); err != nil {
		return err
	}
	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.LCLEAR), key}); err != nil {
		log.Errorf("migrate list sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.ListObj.Del(khash, key); err != nil {
		log.Warnf("migrate list del key:%s err:%s", string(key), err)
	}
	return nil
}

func (m *Migrate) migrateZSet(key []byte, conn redis.Conn) error {
	khash, isHashTag := m.getKeyHash(key)
	spList, err := m.db.ZsetObj.ZRange(key, khash, 0, -1)
	if err != nil {
		log.Errorf("migrate zset zrange key:%s err:%s", string(key), err)
		return err
	} else if len(spList) == 0 {
		return nil
	}

	args := []interface{}{key}
	for i := 0; i < len(spList); i++ {
		args = append(args, spList[i].Score, spList[i].Member)
		if i < len(spList)-1 && i%10 != 0 {
			continue
		}
		if isHashTag {
			hashArgs := make([]interface{}, 0, 3+len(args))
			hashArgs = append(hashArgs, MigrateLuaScript, len(args)+1, resp.ZADD)
			for l := 0; l < len(args); l++ {
				hashArgs = append(hashArgs, args[l])
			}
			if _, err = conn.Do(resp.EVAL, hashArgs...); err != nil {
				log.Errorf("migrate zset send key:%s err:%s", string(key), err)
				return err
			}
		} else {
			if _, err = conn.Do(resp.ZADD, args...); err != nil {
				log.Errorf("migrate zset send key:%s err:%s", string(key), err)
				return err
			}
		}
		args = []interface{}{key}
	}

	if err := m.migrateTTL(key, khash, conn, isHashTag); err != nil {
		return err
	}
	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.ZCLEAR), key}); err != nil {
		log.Errorf("migrate zset sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.ZsetObj.Del(khash, key); err != nil {
		log.Warnf("migrate zset del key:%s err:%s", string(key), err)
	}
	return nil
}

func (m *Migrate) migrateSet(key []byte, conn redis.Conn) error {
	khash, isHashTag := m.getKeyHash(key)
	members, err := m.db.SetObj.SMembers(key, khash)
	if err != nil {
		log.Errorf("migrate set smembers key:%s err:%s", string(key), err)
		return err
	} else if len(members) == 0 {
		return nil
	}

	args := []interface{}{key}
	for i := 0; i < len(members); i++ {
		args = append(args, members[i])
		if i < len(members)-1 && i%10 != 0 {
			continue
		}

		if isHashTag {
			hashArgs := make([]interface{}, 0, 3+len(args))
			hashArgs = append(hashArgs, MigrateLuaScript, len(args)+1, resp.SADD)
			for l := 0; l < len(args); l++ {
				hashArgs = append(hashArgs, args[l])
			}
			if _, err = conn.Do(resp.EVAL, hashArgs...); err != nil {
				log.Errorf("migrate set do key:%s err:%s", string(key), err)
				return err
			}
		} else {
			if _, err = conn.Do(resp.SADD, args...); err != nil {
				log.Errorf("migrate set do key:%s err:%s", string(key), err)
				return err
			}
		}
		args = []interface{}{key}
	}

	if err := m.migrateTTL(key, khash, conn, isHashTag); err != nil {
		return err
	}
	if err := m.migrateDelToSlave(khash, [][]byte{[]byte(resp.SCLEAR), key}); err != nil {
		log.Errorf("migrate set sync slaves key:%s err:%s", string(key), err)
		return err
	}
	if _, err := m.db.SetObj.Del(khash, key); err != nil {
		log.Warnf("migrate set del key:%s err:%s", string(key), err)
	}
	return nil
}

func (m *Migrate) migrateRunTask(isMaster func() bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("migrateTaskRun panic recover err:%v stack:%s", r, string(debug.Stack()))
			err = fmt.Errorf("%v", r)
		}
	}()

	var goros = 10
	var limit = goros * 1000
	var begin []byte
	var list []btools.ScanPair
	var wg sync.WaitGroup

	for {
		begin, list, err = m.db.ScanBySlotId(m.slotId, begin, limit, "*")
		if err != nil {
			log.Warnf("migrate scan slotId:%d err:%s", m.slotId, err.Error())
			return err
		}
		log.Infof("migrate scan slotId:%d begin:%s length:%d", m.slotId, unsafe2.String(begin), len(list))
		for i := 0; i < goros && len(list) > i*1000; i++ {
			wg.Add(1)
			var j = i * 1000
			go func() {
				defer wg.Done()
				if isMaster == nil || !isMaster() {
					err = errors.New("migrate error: server is not master")
					return
				}
				conn := m.Conn.Get()
				defer conn.Close()
				for right := j + 1000; j < len(list) && j < right; j++ {
					key := list[j].Key
					dataType := list[j].Dt
					khash, _ := m.getKeyHash(key)

					func() {
						defer m.keyLocker.LockKey(khash, resp.SET)()

						var e error
						atomic.AddInt64(&m.total, 1)
						switch dataType {
						case btools.STRING:
							e = m.migrateString(key, conn)
						case btools.HASH:
							e = m.migrateHash(key, conn)
						case btools.SET:
							e = m.migrateSet(key, conn)
						case btools.LIST:
							e = m.migrateList(key, conn)
						case btools.ZSET, btools.ZSETOLD:
							e = m.migrateZSet(key, conn)
						}
						if e != nil {
							atomic.AddInt64(&m.fails, 1)
						}
					}()
				}
			}()
		}
		wg.Wait()
		if err != nil {
			return err
		}
		if len(list) < limit || bytes.Equal(begin, btools.ScanEndCurosr) {
			break
		}
	}
	return nil
}

func (m *Migrate) migrateRetryRunTask(isMaster func() bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("migrateTaskRun panic recover err:%v stack:%s", r, string(debug.Stack()))
			err = fmt.Errorf("%v", r)
		}
	}()

	var goros = 10
	var limit = goros * 1000
	var list []btools.ScanPair
	var wg sync.WaitGroup

	for {
		_, list, err = m.db.ScanBySlotId(m.slotId, nil, limit, "*")
		if err != nil {
			log.Warnf("migrateretry scan slotId:%d err:%s", m.slotId, err.Error())
			return err
		}
		log.Infof("migrateretry slotId:%d scanKeyNum:%d", m.slotId, len(list))
		for i := 0; i < goros && len(list) > i*1000; i++ {
			wg.Add(1)
			var j = i * 1000
			go func() {
				defer wg.Done()
				if isMaster == nil || !isMaster() {
					err = errors.New("migrateretry error: server is not master")
					return
				}
				conn := m.Conn.Get()
				defer conn.Close()
				for right := j + 1000; j < len(list) && j < right; j++ {
					key := list[j].Key
					dataType := list[j].Dt
					khash, _ := m.getKeyHash(key)

					func() {
						defer m.keyLocker.LockKey(khash, resp.SET)()

						var e error
						atomic.AddInt64(&m.total, 1)
						switch dataType {
						case btools.STRING:
							e = m.migrateStringRetry(key, conn)
						case btools.HASH:
							e = m.migrateHashRetry(key, conn)
						case btools.SET:
							e = m.migrateSet(key, conn)
						case btools.LIST:
							e = m.migrateList(key, conn)
						case btools.ZSET, btools.ZSETOLD:
							e = m.migrateZSet(key, conn)
						}
						if e != nil {
							atomic.AddInt64(&m.fails, 1)
							log.Infof("migrateretry slotId:%d key:%s dataType:%s fail err:%s", m.slotId, string(key), dataType, e)
						} else {
							log.Infof("migrateretry slotId:%d key:%s dataType:%s success", m.slotId, string(key), dataType)
						}
					}()
				}
			}()
		}
		wg.Wait()
		if err != nil {
			return err
		}
		if len(list) == 0 {
			break
		}
	}
	return nil
}

func (m *Migrate) Info() string {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	fmt.Fprintf(buf, `{`)
	fmt.Fprintf(buf, `"unixtime": %d,`, m.beginTime.Unix())
	fmt.Fprintf(buf, `"costs": %d,`, m.endTime.Sub(m.beginTime)/time.Millisecond)
	fmt.Fprintf(buf, `"from": "%s",`, m.fromHost)
	fmt.Fprintf(buf, `"to": "%s",`, m.toHost)
	fmt.Fprintf(buf, `"slot_id": %d,`, m.slotId)
	fmt.Fprintf(buf, `"status": %d,`, m.status)
	fmt.Fprintf(buf, `"total": %d,`, m.total)
	fmt.Fprintf(buf, `"fails": %d,`, m.fails)
	fmt.Fprintf(buf, `"nonce":""`)
	fmt.Fprintf(buf, `}`)
	return buf.String()
}

func (b *Bitalos) CheckRedirectAndLockFunc(cmd string, key []byte, khash uint32) (bool, func()) {
	if len(key) == 0 || b.Migrate == nil || b.Meta.GetMigrateStatus() == 0 {
		return false, nil
	}
	if s := khash % utils.TotalSlot; s != b.Migrate.slotId {
		return false, nil
	}

	switch cmd {
	case resp.MGET, resp.MSET, resp.INFO, "migrateslots", "migratestatus", "migrateend", "migrateslotsretry", "migrateretryend":
		return false, nil
	}

	lockFunc := b.Migrate.keyLocker.LockKey(khash, cmd)

	if n, _ := b.bitsdb.StringObj.Exists(key, khash); n == 1 {
		return false, lockFunc
	} else {
		return true, lockFunc
	}
}

func (b *Bitalos) Redirect(cmd string, key []byte, reqData [][]byte, rw *resp.Writer) error {
	log.Info("redirect cmd: ", cmd, " key: ", string(key))
	var arg []interface{}
	for _, v := range reqData[1:] {
		arg = append(arg, v)
	}

	conn := b.Migrate.Conn.Get()
	defer conn.Close()

	res, err := conn.Do(cmd, arg...)
	if err != nil {
		log.Warn(err)
	}

	switch res := res.(type) {
	case int64:
		rw.WriteInteger(res)
	case string:
		rw.WriteStatus(res)
	case []byte:
		rw.WriteBulk(res)
	case []interface{}:
		rw.WriteArray(res)
	case nil:
		rw.WriteBulk(nil)
	default:
		log.Warnf("redirect cmd:%s key:%s res:%s", cmd, string(key), res)
		err = errors.New("err return type")
	}
	return err
}

func (b *Bitalos) NewMigrate(slot uint32, tohost string, fromhost string) *Migrate {
	mg := &Migrate{
		fromHost:  fromhost,
		toHost:    tohost,
		slotId:    slot,
		beginTime: time.Now(),
		endTime:   time.Now(),
		keyLocker: locker.NewScopeLocker(false),
		Conn: &redis.Pool{
			MaxIdle: 10,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", tohost)
			},
		},
	}
	return mg
}

func (b *Bitalos) MigrateStart(
	from string, host string, slot uint32, isMaster func() bool, migrateDelToSlave func(uint32, [][]byte) error,
) (*Migrate, error) {
	if b.Migrate == nil {
		if b.Meta.GetMigrateStatus() != MigrateStatusPrepare {
			if b.Meta.GetMigrateSlotid() != uint64(slot) {
				return nil, errn.ErrMigrateRunning
			}
		}
	} else {
		if slot != b.Migrate.slotId {
			if b.Migrate.status == MigrateStatusProcess {
				return nil, errn.ErrMigrateRunning
			}
		}
		if b.Migrate.status == MigrateStatusProcess {
			return b.Migrate, nil
		}
	}

	mg := b.NewMigrate(slot, host, from)
	mg.migrateDelToSlave = migrateDelToSlave
	mg.db = b.bitsdb
	b.Migrate = mg
	b.Migrate.IsMigrate.Store(1)

	isSlotMaster := isMaster != nil && isMaster()

	log.Infof("migrate start toHost:%s slotId:%d isMasterSlot:%v", host, slot, isSlotMaster)
	if isSlotMaster {
		mg.status = MigrateStatusProcess

		task.Run(slot, func(task *task.Task) error {
			defer func() {
				mg.endTime = time.Now()
				b.Migrate.IsMigrate.Store(0)
			}()

			if e := mg.migrateRunTask(isMaster); e != nil {
				log.Errorf("migrate Run err:%s", e.Error())
				mg.status = MigrateStatusError
				return e
			}
			mg.status = MigrateStatusFinish
			return nil
		})
	}
	b.bitsdb.StringObj.BaseDb.BitmapMem.StartMigrate(slot)
	b.Meta.SetMigrateStatus(MigrateStatusProcess)
	b.Meta.SetMigrateSlotid(uint64(slot))
	log.Infof("migrate end toHost:%s slotId:%d", host, slot)
	return b.Migrate, nil
}

func (b *Bitalos) MigrateStartRetry(
	from string, host string, slot uint32, isMaster func() bool, migrateDelToSlave func(uint32, [][]byte) error,
) (*Migrate, error) {
	if b.Migrate == nil {
		if b.Meta.GetMigrateStatus() != MigrateStatusPrepare {
			if b.Meta.GetMigrateSlotid() != uint64(slot) {
				return nil, errn.ErrMigrateRunning
			}
		}
	} else {
		if slot != b.Migrate.slotId {
			if b.Migrate.status == MigrateStatusProcess {
				return nil, errn.ErrMigrateRunning
			}
		}
		if b.Migrate.status == MigrateStatusProcess {
			return b.Migrate, nil
		}
	}

	mg := b.NewMigrate(slot, host, from)
	mg.migrateDelToSlave = migrateDelToSlave
	mg.db = b.bitsdb
	b.Migrate = mg
	b.Migrate.IsMigrate.Store(1)

	isSlotMaster := isMaster != nil && isMaster()

	log.Infof("migrateretry start toHost:%s slotId:%d isMasterSlot:%v", host, slot, isSlotMaster)
	if isSlotMaster {
		mg.status = MigrateStatusProcess

		task.Run(slot, func(task *task.Task) error {
			defer func() {
				mg.endTime = time.Now()
				b.Migrate.IsMigrate.Store(0)
			}()

			if e := mg.migrateRetryRunTask(isMaster); e != nil {
				log.Errorf("migrateretry Run err:%s", e.Error())
				mg.status = MigrateStatusError
				return e
			}
			mg.status = MigrateStatusFinish
			return nil
		})
	}
	b.bitsdb.StringObj.BaseDb.BitmapMem.StartMigrate(slot)
	b.Meta.SetMigrateStatus(MigrateStatusProcess)
	b.Meta.SetMigrateSlotid(uint64(slot))
	log.Infof("migrateretry end toHost:%s slotId:%d", host, slot)
	return b.Migrate, nil
}

func (b *Bitalos) MigrateOver(slotId uint64) error {
	if b.Migrate != nil && uint64(b.Migrate.slotId) != slotId {
		return errn.ErrSlotIdNotMatch
	}
	if b.Migrate != nil {
		log.Infof("migrate over toHost:%s slotId:%d", b.Migrate.toHost, slotId)
	} else {
		log.Infof("migrate over slotId:%d", slotId)
	}
	b.bitsdb.StringObj.BaseDb.BitmapMem.ClearMigrate()
	b.Meta.SetMigrateStatus(MigrateStatusPrepare)
	return nil
}

func (b *Bitalos) MigrateRetryOver(slotId uint64) error {
	if b.Migrate != nil && uint64(b.Migrate.slotId) != slotId {
		return errn.ErrSlotIdNotMatch
	}
	if b.Migrate != nil {
		log.Infof("migrateretryend over toHost:%s slotId:%d", b.Migrate.toHost, slotId)
	} else {
		log.Infof("migrateretryend over slotId:%d", slotId)
	}
	b.bitsdb.StringObj.BaseDb.BitmapMem.ClearMigrate()
	b.Meta.SetMigrateStatus(MigrateStatusPrepare)
	b.Migrate = nil
	return nil
}
