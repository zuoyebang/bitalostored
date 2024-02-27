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

package router

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/sony/gobreaker"
)

func (pc *ProxyClient) doWithClients(commandName string, s *resp.Session, args ...interface{}) (res interface{}, err error) {
	recorder := s.Recorder
	clients := recorder.ServerClients
	switch strings.ToUpper(commandName) {
	case resp.WATCH:
		return execStoredWatch(pc, recorder, commandName, args...)
	case resp.UNWATCH:
		return execStoredUnwatch(pc, clients, commandName)
	case resp.MULTI:
		return execStoredMulti(pc, clients, commandName)
	case resp.PREPARE:
		return execStoredPrepare(pc, clients, commandName)
	case resp.EXEC:
		return execStoredTxExec(pc, recorder, commandName)
	case resp.DISCARD:
		return execStoredDiscard(pc, clients, commandName)
	case resp.MGET:
		if s.TxState&resp.TxStateCancel != 0 {
			return nil, nil
		}
		recorder.CmdNum++
		return execStoredTxMGet(pc, recorder, commandName, args...)
	case resp.MSET:
		if s.TxState&resp.TxStateCancel != 0 {
			return nil, nil
		}
		recorder.CmdNum++
		return execStoredTxMSet(pc, recorder, commandName, args...)
	case resp.DEL:
		if s.TxState&resp.TxStateCancel != 0 {
			return nil, nil
		}
		recorder.CmdNum++
		return execStoredTxDel(pc, recorder, commandName, args...)
	default:
		if s.TxState&resp.TxStateCancel != 0 {
			return nil, nil
		}
		slotId := pc.router.Hash(args[0])
		gid := pc.router.GetSlot(slotId).MasterAddrGroupId
		if conn, ok := clients[gid]; ok {
			res, err = goStoredDoTx(pc, conn, commandName, args...)
			if err == nil {
				recorder.CmdNum++
				recorder.AddCommand(gid)
			}
		} else {
			return nil, resp.TxGroupChangedErr
		}
	}
	return res, err
}

func goStoredDoTx(r *ProxyClient, conn *resp.InternalServerConn, commandName string, args ...interface{}) (res interface{}, err error) {
	isWrite := IsWriteCmd(commandName)
	if r.readOnly && isWrite {
		return nil, resp.WriteErrorOnReadOnlyProxy
	}
	res, err = conn.Conn.Do(commandName, args...)
	if err != nil {
		log.Warnf("command:%s args:%+v res:%+v err:%+v", commandName, args, res, err)
	}
	return res, err
}

func (pc *ProxyClient) do(commandName string, s *resp.Session, args ...interface{}) (res interface{}, err error) {
	if s != nil && s.OpenDistributedTx && s.TxCommandQueued {
		return pc.doWithClients(commandName, s, args...)
	}

	switch strings.ToUpper(commandName) {
	case resp.MGET:
		return execStoredMGet(pc, commandName, args...)
	case resp.DEL:
		return execStoredDel(pc, commandName, args...)
	case resp.MSET:
		return execStoredMSet(pc, commandName, args...)
	case resp.EVALSHA, resp.EVAL:
		var slotId int
		if len(args) <= 2 {
			slotId = pc.router.Hash("")
		} else {
			slotId = pc.router.HashForLua(args[2].(string))
		}
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	case resp.SCRIPT:
		var slotId int
		switch strings.ToUpper(args[0].(string)) {
		case "LOAD":
			if len(args) != 2 {
				return nil, resp.CmdParamsErr(resp.SCRIPT)
			}
			return broadcastAllGroup(pc, commandName, args...)
		case "FLUSH":
			if len(args) != 1 {
				return nil, resp.CmdParamsErr(resp.SCRIPT)
			}
			return broadcastAllGroup(pc, commandName, args...)
		case "EXISTS":
			if len(args) == 1 {
				slotId = pc.router.Hash("")
			} else {
				slotId = pc.router.Hash(args[1].(string))
			}
			res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
		case "LEN":
			if len(args) == 1 {
				slotId = pc.router.Hash("")
			} else {
				slotId = pc.router.Hash(args[1].(string))
			}
			res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
		default:
			return nil, resp.CmdParamsErr(resp.SCRIPT)
		}
	case resp.LRANGE:
		slotId := pc.router.Hash(args[0])
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	case resp.HKEYS, resp.HGETALL:
		slotId := pc.router.Hash(args[0])
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	case resp.SMEMBERS:
		slotId := pc.router.Hash(args[0])
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	case resp.ZRANGE, resp.ZREVRANGE:
		slotId := pc.router.Hash(args[0])
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	case resp.ZRANGEBYSCORE, resp.ZREVRANGEBYSCORE, resp.ZRANK, resp.ZREVRANK:
		slotId := pc.router.Hash(args[0])
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	default:
		slotId := pc.router.Hash(args[0])
		res, err, _ = goStoredDo(pc, slotId, commandName, nil, args...)
	}
	return res, err
}

func goStoredDo(r *ProxyClient, slotId int, commandName string, prevGetConn func() (*InternalPool, bool, uint64, string, error), args ...interface{}) (res interface{}, err error, addrs string) {
	isWrite := IsWriteCmd(commandName)
	if r.readOnly && isWrite {
		return nil, resp.WriteErrorOnReadOnlyProxy, ""
	}
	var storedAddrPool *InternalPool
	var needCircuit bool
	var curindex uint64
	var cloudType string
	if prevGetConn != nil {
		storedAddrPool, needCircuit, curindex, cloudType, err = prevGetConn()
	} else {
		storedAddrPool, needCircuit, curindex, cloudType, err = r.router.GetConn(slotId, commandName)
	}
	if err != nil {
		log.Warnf("get stored conn fail slotId:%d commandName:%s args:%v err:%v", slotId, commandName, args, err)
		return nil, err, ""
	}
	hystrixName := storedAddrPool.GetHostPort()
	doCmdFunc := func() (interface{}, error) {
		conn := storedAddrPool.GetConn()
		res, err := conn.Do(commandName, args...)
		defer conn.Close()
		if err != nil {
			log.Warnf("do redis cmd fail addr:%s slotId:%d commandName:%s args:%s err:%v", hystrixName, slotId, commandName, args, err)
			return nil, err
		}
		return res, nil
	}

	if !needCircuit {
		res, err = doCmdFunc()
		return res, err, hystrixName
	}

	var cgb *Breaker
	groupId, _ := r.router.GetGroupId(slotId)
	cgb, err = r.router.GroupBreaker.GetCircuitBreakerByGid(groupId)
	if cgb == nil || err != nil {
		log.Warnf("get group circuit breaker fail err:%v", err)
		res, err = doCmdFunc()
		return res, err, hystrixName
	}

	cb := cgb.GetCircuitBreaker(hystrixName)
	if cb == nil {
		log.Warnf("get circuit breaker fail cb is nil addr:%s", hystrixName)
		res, err = doCmdFunc()
		return res, err, hystrixName
	}

	reacquirePoolAndBreaker := func() error {
		storedAddrPool, curindex, err = r.router.GetAnotherConnByCircuit(slotId, commandName, curindex, cloudType)
		if err != nil {
			return err
		}
		hystrixName = storedAddrPool.GetHostPort()
		cb = cgb.GetCircuitBreaker(hystrixName)
		if cb == nil {
			return errors.New("circuit open, get another ip failed")
		}
		return nil
	}

	state := cb.State()
	if state == gobreaker.StateOpen {
		err = reacquirePoolAndBreaker()
		if err != nil {
			return nil, err, hystrixName
		}
		res, err = cb.Execute(doCmdFunc)
	} else {
		res, err = cb.Execute(doCmdFunc)
		if state == gobreaker.StateHalfOpen && err == gobreaker.ErrTooManyRequests {
			err = reacquirePoolAndBreaker()
			if err != nil {
				return nil, err, hystrixName
			}
			res, err = cb.Execute(doCmdFunc)
		}
	}

	return res, err, hystrixName
}

func broadcastAllGroup(pc *ProxyClient, command string, args ...interface{}) (interface{}, error) {
	groupMap := make(map[int]bool, 1)
	var res interface{}
	var err error
	for _, slot := range pc.router.slots {
		if !groupMap[slot.MasterAddrGroupId] {
			groupMap[slot.MasterAddrGroupId] = true
			if res, err, _ = goStoredDo(pc, slot.Id, command, nil, args...); err != nil {
				return res, err
			}
		}
	}
	return res, err
}

type antsCommandParams struct {
	wg             *sync.WaitGroup
	r              *ProxyClient
	prevGetConn    func() (*InternalPool, bool, uint64, string, error)
	slotId         int
	commandName    string
	commandType    int
	newArgs        []interface{}
	slotKeysMap    map[int][]interface{}
	slotIndexesMap map[int][]int
	result         []interface{}
	delNum         *int64
	seq            int
	isTx           bool
	txResult       *sync.Map
	txErr          *sync.Map
	masterConn     *resp.InternalServerConn
}

func multiCommandAntsCallback(params interface{}) {
	m, ok := params.(*antsCommandParams)
	if !ok {
		return
	}
	if m.wg != nil {
		defer m.wg.Done()
	}
	if !m.isTx {
		switch m.commandType {
		case MgetCommandType:
			res, err, addr := goStoredDo(m.r, m.slotId, m.commandName, m.prevGetConn, m.newArgs...)
			if err != nil {
				return
			}
			resTmp, errTmp := redis.ByteSlices(res, err)
			if errTmp != nil {
				log.Warnf("mget failed addr:%s slotId:%d params:%v err:%v resLen:%d", addr, m.slotId, m.newArgs, errTmp, len(resTmp))
				return
			}
			resIndexs, _ := m.slotIndexesMap[m.slotId]

			for i := range resTmp {
				m.result[resIndexs[i]] = resTmp[i]
			}
		case MsetCommandType:
			_, err, addr := goStoredDo(m.r, m.slotId, m.commandName, m.prevGetConn, m.newArgs...)
			if err != nil {
				log.Warnf("mset failed addr:%s command:%s args:%s err:%s", addr, m.commandName, m.newArgs, err.Error())
				return
			}
		case DelCommandType:
			res, err, addr := goStoredDo(m.r, m.slotId, m.commandName, m.prevGetConn, m.newArgs...)
			if err != nil {
				log.Warnf("del failed addr:%s command:%s args:%s err:%s", addr, m.commandName, m.newArgs, err.Error())
				return
			}
			groupRet, err := redis.Int64(res, nil)
			if err != nil {
				return
			}
			atomic.AddInt64(m.delNum, groupRet)
		}
	} else {
		switch m.commandType {
		case WatchCommandType:
			_, err := goStoredDoTx(m.r, m.masterConn, m.commandName, m.newArgs...)
			m.result[m.seq] = err
		case UnwatchCommandType:
			goStoredDoTx(m.r, m.masterConn, m.commandName)
		case MultiCommandType:
			_, err := goStoredDoTx(m.r, m.masterConn, m.commandName)
			m.result[m.seq] = err
		case PrepareCommandType:
			_, err := goStoredDoTx(m.r, m.masterConn, m.commandName)
			m.result[m.seq] = err
		case ExecCommandType:
			groupId := m.masterConn.GroupId
			if res, err := goStoredDoTx(m.r, m.masterConn, m.commandName); err != nil {
				log.Warnf("goStoredDoTx fail addr:%s err:%s", m.masterConn.HostPort, err.Error())
				m.txErr.Store(groupId, err)
			} else {
				if resSlice, ok := res.([]interface{}); ok {
					m.txResult.Store(groupId, resSlice)
				} else {
					m.txResult.Store(groupId, nil)
				}
			}
		case MgetCommandType, DelCommandType, MsetCommandType:
			goStoredDoTx(m.r, m.masterConn, m.commandName, m.newArgs...)
		}
	}
}

func execStoredMSet(r *ProxyClient, commandName string, args ...interface{}) (interface{}, error) {
	slotNameKVsMap := divideStoredKeysValues(r, args...)

	var wg sync.WaitGroup
	for slotId, newArgs := range slotNameKVsMap {
		storedAddrPool, needCircuit, curindex, cloudType, err := r.router.GetConn(slotId, commandName)
		if err != nil {
			log.Warnf("get stored conn fail slotId:%d commandName:%s err:%s", slotId, commandName, err.Error())
			return nil, err
		}
		addr := storedAddrPool.GetHostPort()
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail slotId:%d commandName:%s addr:%s", slotId, commandName, addr)
			continue
		}

		m := &antsCommandParams{
			wg:     &wg,
			r:      r,
			slotId: slotId,
			prevGetConn: func() (*InternalPool, bool, uint64, string, error) {
				return storedAddrPool, needCircuit, curindex, cloudType, nil
			},
			commandName: commandName,
			commandType: MsetCommandType,
			newArgs:     newArgs,
		}

		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("antsPools invoke failed command:%s addr:%s antsPoolRunning:%d err:%s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return nil, nil
}

func execStoredMGet(r *ProxyClient, commandName string, args ...interface{}) (interface{}, error) {
	slotKeysMap, slotIndexesMap := divideStoredOnlyKeys(r, args...)

	result := make([]interface{}, len(args), len(args))
	wg := sync.WaitGroup{}

	for slotId, newArgs := range slotKeysMap {
		storedAddrPool, needCircuit, curindex, cloudType, err := r.router.GetConn(slotId, commandName)
		if err != nil {
			log.Warnf("execStoredMGet GetConn failed slotId:%d commandName:%s err:%s", slotId, commandName, err.Error())
			return nil, err
		}
		addr := storedAddrPool.GetHostPort()
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			storedAddrPool, needCircuit, curindex, cloudType, err = r.router.GetConn(slotId, commandName)
			addr = storedAddrPool.GetHostPort()
			antsPool, ok = r.router.GetAntsPool(addr)
			if !ok {
				log.Warnf("execStoredMGet GetAntsPool failed slotId:%d commandName:%s addr:%s", slotId, commandName, addr)
				continue
			}
		}

		var cgb *Breaker
		hystrixName := storedAddrPool.GetHostPort()
		groupId, _ := r.router.GetGroupId(slotId)
		cgb, err = r.router.GroupBreaker.GetCircuitBreakerByGid(groupId)
		m := &antsCommandParams{
			wg:     &wg,
			r:      r,
			slotId: slotId,
			prevGetConn: func() (*InternalPool, bool, uint64, string, error) {
				return storedAddrPool, needCircuit, curindex, cloudType, err
			},
			commandName:    commandName,
			commandType:    MgetCommandType,
			newArgs:        newArgs,
			slotKeysMap:    slotKeysMap,
			slotIndexesMap: slotIndexesMap,
			result:         result,
		}
		doFunc := func() (interface{}, error) {
			wg.Add(1)
			err := antsPool.Invoke(m)
			if err != nil {
				wg.Done()
				log.Warnf("execStoredMGet antsPools invoke failed addr:%s antsPoolRunning:%d err:%s", addr, antsPool.Running(), err.Error())
			}
			return nil, err
		}

		cb := cgb.GetCircuitBreaker(hystrixName)
		if cb != nil {
			if cb.State() == gobreaker.StateOpen {
				storedAddrPool, _, err = r.router.GetAnotherConnByCircuit(slotId, commandName, curindex, cloudType)
				if err != nil {
					log.Warnf("execStoredMGet get another break failed addr:%s err:%s", hystrixName, err.Error())
					continue
				}

				hystrixName = storedAddrPool.GetHostPort()
				cb = cgb.GetCircuitBreaker(hystrixName)
				if cb == nil {
					log.Warnf("first circuit open get another failed addr:%s", hystrixName)
					continue
				}
			}
		}

		doFunc()
	}
	wg.Wait()
	return result, nil
}

func execStoredDel(r *ProxyClient, commandName string, args ...interface{}) (interface{}, error) {
	hostKeysMap, _ := divideStoredOnlyKeys(r, args...)
	var result int64 = 0
	var wg sync.WaitGroup
	for slotId, newArgs := range hostKeysMap {
		storedAddrPool, needCircuit, curindex, cloudType, err := r.router.GetConn(slotId, commandName)
		if err != nil {
			log.Warnf("get stored conn fail, slotId:%d, commandName:%s, err:%s", slotId, commandName, err)
			continue
		}

		addr := storedAddrPool.GetHostPort()
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, slotId:%d, commandName:%s, addr:%s", slotId, commandName, addr)
			continue
		}

		m := &antsCommandParams{
			wg:     &wg,
			r:      r,
			slotId: slotId,
			prevGetConn: func() (*InternalPool, bool, uint64, string, error) {
				return storedAddrPool, needCircuit, curindex, cloudType, nil
			},
			commandName: commandName,
			commandType: DelCommandType,
			newArgs:     newArgs,
			delNum:      &result,
		}
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return result, nil
}

func execStoredWatch(r *ProxyClient, recorder *resp.TxRecorder, commandName string, args ...interface{}) (interface{}, error) {
	hostKeysMap, _, err := divideGroupOnlyKeys(r, recorder, args...)
	if err != nil {
		return nil, err
	}

	clients := recorder.ServerClients
	var wg sync.WaitGroup
	result := make([]interface{}, len(hostKeysMap))
	idx := 0
	for gid, newArgs := range hostKeysMap {
		addr := clients[gid].HostPort
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, gid:%d, commandName:%s, addr:%s", gid, commandName, addr)
			result[idx] = errn.ErrAntsPoolGetFail
			idx++
			continue
		}

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: WatchCommandType,
			newArgs:     newArgs,
			seq:         idx,
			result:      result,
			isTx:        true,
			masterConn:  clients[gid],
		}
		idx++
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()

	for _, r := range result {
		if r != nil {
			if e, ok := r.(error); !ok {
				log.Warnf("watch result not err. result:%+v", r)
				return nil, errn.ErrWatchResultErr
			} else {
				log.Warnf("watch result is err: %+v", e)
				return nil, e
			}
		}
	}
	return nil, nil
}

func execStoredUnwatch(r *ProxyClient, clients map[int]*resp.InternalServerConn, commandName string) (interface{}, error) {
	var wg sync.WaitGroup
	var err error
	for gid, client := range clients {
		addr := client.HostPort
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, gid:%d, commandName:%s, addr:%s", gid, commandName, addr)
			continue
		}

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: UnwatchCommandType,
			isTx:        true,
			masterConn:  client,
		}
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return nil, nil
}

func execStoredMulti(r *ProxyClient, clients map[int]*resp.InternalServerConn, commandName string) (interface{}, error) {
	var wg sync.WaitGroup
	var err error
	result := make([]interface{}, len(clients))
	idx := 0
	for gid, client := range clients {
		addr := client.HostPort
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, gid:%d, commandName:%s, addr:%s", gid, commandName, addr)
			result[idx] = errn.ErrAntsPoolGetFail
			idx++
			continue
		}

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			result:      result,
			commandName: commandName,
			commandType: MultiCommandType,
			isTx:        true,
			masterConn:  client,
			seq:         idx,
		}
		idx++
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	for _, r := range result {
		if r != nil {
			if e, ok := r.(error); !ok {
				log.Warnf("multi result not err. result:%+v", r)
				return nil, errn.ErrMultiResultErr
			} else if e.Error() != errn.ErrMultiNested.Error() {
				log.Warnf("multi result is err: %+v", e)
				return nil, e
			}
		}
	}
	return nil, nil
}

func execStoredPrepare(r *ProxyClient, clients map[int]*resp.InternalServerConn, commandName string) (interface{}, error) {
	var wg sync.WaitGroup
	var err error
	result := make([]interface{}, len(clients))
	idx := 0
	for gid, client := range clients {
		addr := client.HostPort
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, gid:%d, commandName:%s, addr:%s", gid, commandName, addr)
			result[idx] = errn.ErrAntsPoolGetFail
			idx++
			continue
		}

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: PrepareCommandType,
			result:      result,
			seq:         idx,
			isTx:        true,
			masterConn:  client,
		}
		idx++
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()

	for _, r := range result {
		if r != nil {
			if e, ok := r.(error); !ok {
				log.Warnf("prepare result not err. result:%+v", r)
				return nil, errn.ErrPrepareFail
			} else {
				log.Warnf("prepare result is err: %+v", e)
				return nil, e
			}
		}
	}
	return nil, nil
}

func execStoredDiscard(r *ProxyClient, clients map[int]*resp.InternalServerConn, commandName string) (interface{}, error) {
	var wg sync.WaitGroup
	var err error
	for gid, client := range clients {
		addr := client.HostPort
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, gid:%d, commandName:%s, addr:%s", gid, commandName, addr)
			continue
		}

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: DiscardCommandType,
			isTx:        true,
			masterConn:  client,
		}
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return nil, nil
}

func execStoredTxExec(r *ProxyClient, recorder *resp.TxRecorder, commandName string) (interface{}, error) {
	var wg sync.WaitGroup
	var err error
	var txErr sync.Map
	var txResult sync.Map
	for gid, client := range recorder.ServerClients {
		if n, ok := recorder.ServerCmdNum[gid]; !ok || n == 0 {
			continue
		}
		addr := client.HostPort
		antsPool, ok := r.router.GetAntsPool(addr)
		if !ok {
			log.Warnf("antsPools load fail, gid:%d, commandName:%s, addr:%s", gid, commandName, addr)
			continue
		}

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: ExecCommandType,
			txErr:       &txErr,
			txResult:    &txResult,
			isTx:        true,
			masterConn:  client,
		}
		wg.Add(1)
		err = antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, command: %s, addr: %s, current running task: %d, err: %s", commandName, addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()

	for _, cmdRes := range recorder.TxCommands {
		switch cmdRes.Command {
		case resp.MgetCmdType:
			{
				newResult := make([]interface{}, cmdRes.KeyNum)
				for gid, groupSeq := range cmdRes.ServerRespSeq {
					if _, ok := txErr.Load(gid); ok {
						for _, pos := range cmdRes.RespGroupMap[gid] {
							newResult[pos] = nil
						}
						continue
					}

					r, ok := txResult.Load(gid)
					if !ok {
						continue
					}
					resSlice, ok := r.([]interface{})
					if !ok || resSlice == nil {
						continue
					}
					if resSlice[groupSeq] == nil {
						for _, pos := range cmdRes.RespGroupMap[gid] {
							newResult[pos] = nil
						}
						continue
					}

					serverResult := resSlice[groupSeq].([]interface{})
					if len(cmdRes.RespGroupMap[gid]) != len(serverResult) {
						log.Warnf("tx result len(%d) != expect len(%d)", len(serverResult), len(cmdRes.RespGroupMap[gid]))
						continue
					}
					for i, pos := range cmdRes.RespGroupMap[gid] {
						newResult[pos] = serverResult[i]
					}
				}
				cmdRes.Response = newResult
			}
		case resp.DelCmdType:
			{
				delNum := int64(0)
				var groupDelRet int64
				for gid, groupSeq := range cmdRes.ServerRespSeq {
					if _, ok := txErr.Load(gid); ok {
						continue
					}

					r, ok := txResult.Load(gid)
					if !ok {
						continue
					}
					resSlice, ok := r.([]interface{})
					if !ok || resSlice == nil {
						continue
					}

					if groupSeq < len(resSlice) {
						groupDelRet = resSlice[groupSeq].(int64)
						if groupDelRet > 0 {
							delNum += groupDelRet
						}
					}
				}
				cmdRes.Response = delNum
			}
		case resp.MsetCmdType:
			{
				var ret interface{}
				for gid, groupSeq := range cmdRes.ServerRespSeq {
					if _, ok := txErr.Load(gid); ok {
						continue
					}
					r, ok := txResult.Load(gid)
					if !ok {
						continue
					}
					resSlice, ok := r.([]interface{})
					if !ok || resSlice == nil {
						continue
					}
					if resSlice[groupSeq] == nil {
						continue
					}
					ret = resSlice[groupSeq]
					switch ret.(type) {
					case error:
						log.Warnf("mset result: gid:%d seq:%d ret:%+v", gid, groupSeq, ret)
					}
				}
				cmdRes.Response = resp.ReplyOK
			}
		default:
			gid := cmdRes.GroupId
			groupSeq := cmdRes.ServerRespSeq[cmdRes.GroupId]
			if err, ok := txErr.Load(gid); ok {
				cmdRes.Response = err
			} else {
				r, ok := txResult.Load(gid)
				if !ok {
					continue
				}
				resSlice, ok := r.([]interface{})
				if !ok || resSlice == nil {
					continue
				}
				cmdRes.Response = resSlice[groupSeq]
			}
		}
	}
	return nil, nil
}

func execStoredTxMGet(r *ProxyClient, recorder *resp.TxRecorder, commandName string, args ...interface{}) (interface{}, error) {
	slotKeysMap, slotIndexesMap, err := divideGroupOnlyKeys(r, recorder, args...)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(args), len(args))
	wg := sync.WaitGroup{}

	var serverMap resp.TxCommandServerMap
	serverMap.Command = resp.MgetCmdType
	serverMap.KeyNum = len(args)
	serverMap.RespGroupMap = slotIndexesMap
	serverMap.ServerRespSeq = make(map[int]int, len(slotKeysMap))
	for groupId := range slotKeysMap {
		serverMap.ServerRespSeq[groupId] = recorder.ServerCmdNum[groupId]
		recorder.ServerCmdNum[groupId]++
	}
	recorder.TxCommands = append(recorder.TxCommands, &serverMap)

	for groupId, newArgs := range slotKeysMap {
		conn := recorder.ServerClients[groupId]
		addr := recorder.ServerClients[groupId].HostPort
		antsPool, _ := r.router.GetAntsPool(addr)

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: MgetCommandType,
			newArgs:     newArgs,
			result:      result,
			masterConn:  conn,
			isTx:        true,
		}

		wg.Add(1)
		err := antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, addr: %s, current running task: %d, err: %s", addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return result, nil
}

func execStoredTxMSet(r *ProxyClient, recorder *resp.TxRecorder, commandName string, args ...interface{}) (interface{}, error) {
	groupKvMap, err := divideGroupKeysValues(r, recorder, args...)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	var serverMap resp.TxCommandServerMap
	serverMap.Command = resp.MsetCmdType
	serverMap.ServerRespSeq = make(map[int]int, len(groupKvMap))
	for groupId := range groupKvMap {
		serverMap.ServerRespSeq[groupId] = recorder.ServerCmdNum[groupId]
		recorder.ServerCmdNum[groupId]++
	}
	recorder.TxCommands = append(recorder.TxCommands, &serverMap)

	for groupId, newArgs := range groupKvMap {
		conn := recorder.ServerClients[groupId]
		addr := recorder.ServerClients[groupId].HostPort
		antsPool, _ := r.router.GetAntsPool(addr)

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: MsetCommandType,
			newArgs:     newArgs,
			masterConn:  conn,
			isTx:        true,
		}

		wg.Add(1)
		err := antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, addr: %s, current running task: %d, err: %s", addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return nil, nil
}

func execStoredTxDel(r *ProxyClient, recorder *resp.TxRecorder, commandName string, args ...interface{}) (interface{}, error) {
	slotKeysMap, _, err := divideGroupOnlyKeys(r, recorder, args...)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(slotKeysMap), len(slotKeysMap))
	wg := sync.WaitGroup{}

	var serverMap resp.TxCommandServerMap
	serverMap.Command = resp.DelCmdType
	serverMap.KeyNum = len(args)
	serverMap.ServerRespSeq = make(map[int]int, len(slotKeysMap))
	for groupId := range slotKeysMap {
		serverMap.ServerRespSeq[groupId] = recorder.ServerCmdNum[groupId]
		recorder.ServerCmdNum[groupId]++
	}
	recorder.TxCommands = append(recorder.TxCommands, &serverMap)

	for groupId, newArgs := range slotKeysMap {
		conn := recorder.ServerClients[groupId]
		addr := recorder.ServerClients[groupId].HostPort
		antsPool, _ := r.router.GetAntsPool(addr)

		m := &antsCommandParams{
			wg:          &wg,
			r:           r,
			commandName: commandName,
			commandType: DelCommandType,
			newArgs:     newArgs,
			slotKeysMap: slotKeysMap,
			masterConn:  conn,
			isTx:        true,
		}

		wg.Add(1)
		err := antsPool.Invoke(m)
		if err != nil {
			wg.Done()
			log.Warnf("ants pool invoke failed, addr: %s, current running task: %d, err: %s", addr, antsPool.Running(), err.Error())
		}
	}
	wg.Wait()
	return result, nil
}

func divideStoredOnlyKeys(r *ProxyClient, args ...interface{}) (map[int][]interface{}, map[int][]int) {
	slotMap := make(map[int][]interface{}, len(args))
	slotIndexMap := make(map[int][]int, len(args))
	groupHeadSlot := make(map[int]int, 0)
	var slotId int
	for i, keyInterface := range args {
		slotId = r.router.Hash(keyInterface)

		if slotId < 0 {
			continue
		}

		slot := r.router.GetSlot(slotId)

		if headSlotId, ok := groupHeadSlot[slot.MasterAddrGroupId]; ok {
			slotId = headSlotId
		} else {
			groupHeadSlot[slot.MasterAddrGroupId] = slotId
		}
		if _, ok := slotMap[slotId]; ok {
			slotMap[slotId] = append(slotMap[slotId], keyInterface)
			slotIndexMap[slotId] = append(slotIndexMap[slotId], i)
		} else {
			slotMap[slotId] = make([]interface{}, 0, len(args))
			slotMap[slotId] = append(slotMap[slotId], keyInterface)
			slotIndexMap[slotId] = make([]int, 0, len(args))
			slotIndexMap[slotId] = append(slotIndexMap[slotId], i)
		}
	}
	return slotMap, slotIndexMap
}

func divideGroupOnlyKeys(r *ProxyClient, recorder *resp.TxRecorder, args ...interface{}) (map[int][]interface{}, map[int][]int, error) {
	slotMap := make(map[int][]interface{}, len(args))
	slotIndexMap := make(map[int][]int, len(args))
	var slotId int
	var err error
	for i, keyInterface := range args {
		slotId = r.router.Hash(keyInterface)
		if slotId < 0 {
			continue
		}

		slot := r.router.GetSlot(slotId)
		gid := slot.MasterAddrGroupId
		if _, ok := recorder.ServerClients[gid]; !ok {
			return nil, nil, resp.TxGroupChangedErr
		}
		if _, ok := slotMap[gid]; ok {
			slotMap[gid] = append(slotMap[gid], keyInterface)
			slotIndexMap[gid] = append(slotIndexMap[gid], i)
		} else {
			slotMap[gid] = make([]interface{}, 0, len(args))
			slotMap[gid] = append(slotMap[gid], keyInterface)
			slotIndexMap[gid] = make([]int, 0, len(args))
			slotIndexMap[gid] = append(slotIndexMap[gid], i)
		}
	}
	return slotMap, slotIndexMap, err
}

func divideStoredKeysValues(r *ProxyClient, args ...interface{}) map[int][]interface{} {
	slotMap := make(map[int][]interface{}, len(args))
	groupHeadSlot := make(map[int]int, 0)
	for i := 0; i < len(args); i = i + 2 {
		if key, ok := args[i].(string); ok {
			slotId := r.router.Hash(key)
			slot := r.router.GetSlot(slotId)
			if headSlotId, ok := groupHeadSlot[slot.MasterAddrGroupId]; ok {
				slotId = headSlotId
			} else {
				groupHeadSlot[slot.MasterAddrGroupId] = slotId
			}
			if _, ok := slotMap[slotId]; ok {
				slotMap[slotId] = append(slotMap[slotId], key, args[i+1])
			} else {
				slotMap[slotId] = make([]interface{}, 0, len(args))
				slotMap[slotId] = append(slotMap[slotId], key, args[i+1])
			}
		}
	}
	return slotMap
}

func divideGroupKeysValues(r *ProxyClient, recorder *resp.TxRecorder, args ...interface{}) (map[int][]interface{}, error) {
	slotMap := make(map[int][]interface{}, len(args))
	for i := 0; i < len(args); i = i + 2 {
		if key, ok := args[i].(string); ok {
			slotId := r.router.Hash(key)
			slot := r.router.GetSlot(slotId)
			gid := slot.MasterAddrGroupId

			if _, ok := recorder.ServerClients[gid]; !ok {
				return nil, resp.TxGroupChangedErr
			}

			if _, ok := slotMap[gid]; !ok {
				slotMap[gid] = make([]interface{}, 0, len(args))
			}
			slotMap[gid] = append(slotMap[gid], key, args[i+1])
		}
	}
	return slotMap, nil
}
