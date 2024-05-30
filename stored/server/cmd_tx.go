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

package server

import (
	"time"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.WATCH:   {Sync: false, Handler: watchCommand, NotAllowedInTx: true},
		resp.UNWATCH: {Sync: false, Handler: unwatchCommand, NotAllowedInTx: true, NoKey: true},
		resp.MULTI:   {Sync: false, Handler: multiCommand, NoKey: true},
		resp.PREPARE: {Sync: false, Handler: prepareCommand, NoKey: true},
		resp.EXEC:    {Sync: false, Handler: execCommand, NoKey: true},
		resp.DISCARD: {Sync: false, Handler: discardCommand, NoKey: true},
	})
}

func watchCommand(c *Client) error {
	if !c.server.openDistributedTx {
		return errn.ErrTxDisable
	}
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.WATCH)
	}
	if !c.server.IsMaster() {
		return errn.ErrTxNotInMaster
	}

	c.txState |= TxStateWatch

	var khash uint32
	for i := range args {
		if i == 0 {
			khash = c.KeyHash
		} else {
			khash = hash.Fnv32(args[i])
		}
		c.addWatchKey(c.server.txLocks.GetTxLock(khash), args[i], c.QueryStartTime)
	}
	c.RespWriter.WriteStatus("OK")
	return nil
}

func unwatchCommand(c *Client) error {
	if !c.server.openDistributedTx {
		return errn.ErrTxDisable
	}
	if len(c.Args) != 0 {
		return resp.CmdParamsErr(resp.WATCH)
	}
	if c.txState&TxStateWatch != 0 {
		c.txState &= ^(TxStateWatch)
		c.unwatchKey()
	}
	c.RespWriter.WriteStatus("OK")
	return nil
}

func multiCommand(c *Client) error {
	if !c.server.openDistributedTx {
		return errn.ErrTxDisable
	}
	if len(c.Args) != 0 {
		return resp.CmdParamsErr(resp.MULTI)
	}
	if c.txState&TxStateMulti != 0 {
		return errn.ErrMultiNested
	}
	if !c.server.IsMaster() {
		return errn.ErrTxNotInMaster
	}
	if c.server.txParallelCounter.Load() > utils.TxParallelLimit {
		return errn.ErrTxQpsLimit
	}

	c.txState |= TxStateMulti
	c.enableCommandQueued()
	c.server.txParallelCounter.Add(1)
	c.RespWriter.WriteStatus("OK")
	return nil
}

func prepareCommand(c *Client) error {
	if !c.server.openDistributedTx {
		return errn.ErrTxDisable
	}
	if len(c.Args) != 0 {
		return resp.CmdParamsErr(resp.PREPARE)
	}
	if c.txState&TxStateMulti == 0 {
		return errn.ErrPrepareNoMulti
	}
	if c.txState&TxStatePrepare != 0 {
		return errn.ErrPrepareNested
	}

	err := c.prepareSetLock()
	if err != nil {
		c.unwatchKey()
		c.resetTx()
		return err
	}

	c.txState |= TxStatePrepare
	c.RespWriter.WriteStatus("OK")
	return nil
}

func (c *Client) prepareSetLock() (err error) {
	c.hasPrepareLock.Store(false)
	c.prepareState.Store(PrepareStateNone)

	if len(c.commandQueue) == 0 && len(c.watchKeys) == 0 {
		return nil
	}

	watchKeyModified := false
	for keyStr := range c.watchKeys {
		wk := c.server.txLocks.GetWatchKey(keyStr)
		if c.watchKeys[keyStr] < wk.modifyTs.Load() {
			watchKeyModified = true
			break
		}
	}
	if watchKeyModified {
		c.prepareState.Store(PrepareStateKeyModified)
		return errn.ErrWatchKeyChanged
	}
	if len(c.commandQueue) == 0 {
		return nil
	}

	getAllLockKeys := func() map[string]bool {
		if len(c.commandQueue) <= 0 {
			return nil
		}
		updateKeys := make(map[string]bool, len(c.commandQueue))
		for _, args := range c.commandQueue {
			cmdName := unsafe2.String(args[0])
			commandInfo := c.server.GetCommand(cmdName)
			if commandInfo == nil || !resp.IsWriteCmd(cmdName) || commandInfo.NoKey {
				continue
			}
			argNum := len(args)
			firstPos := 1
			for pos := firstPos; pos < argNum; pos += int(commandInfo.KeySkip) {
				keyStr := unsafe2.String(args[pos])
				if _, ok := c.watchKeys[keyStr]; !ok {
					updateKeys[keyStr] = true
				}
				if commandInfo.KeySkip == 0 {
					break
				}
			}
		}
		return updateKeys
	}
	updateKeys := getAllLockKeys()
	if len(c.watchKeys) == 0 && len(updateKeys) == 0 {
		return nil
	}

	prepareLockOk := true
	watchLockers := make([]*TxWatchKey, 0, len(c.watchKeys)+len(updateKeys))
	var releaseFunc, unlockFunc func()

	unlockFunc = func() {
		for i := len(watchLockers) - 1; i >= 0; i-- {
			watchLockers[i].mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
	}

	for try := 0; try < 3; try++ {
		for keyStr := range c.watchKeys {
			wk := c.server.txLocks.GetWatchKey(keyStr)
			if !wk.mu.TryLock() {
				prepareLockOk = false
				break
			}
			watchLockers = append(watchLockers, wk)
		}
		if !prepareLockOk {
			unlockFunc()
			continue
		}

		for keyStr := range updateKeys {
			txLock := c.server.txLocks.GetTxLockByKey(unsafe2.ByteSlice(keyStr))
			wk := txLock.addWatchKey(c, keyStr, false)
			if !wk.mu.TryLock() {
				prepareLockOk = false
				break
			}
			watchLockers = append(watchLockers, wk)
		}
		break
	}

	releaseFunc = func() {
		lockNum := len(watchLockers)
		if lockNum <= 0 {
			return
		}

		for i := lockNum - 1; i >= 0; i-- {
			txLocker := c.server.txLocks.GetTxLockByKey(unsafe2.ByteSlice(watchLockers[i].key))

			watchLockers[i].mu.Unlock()
			txLocker.removeWatchKey(c, watchLockers[i].key)
		}
	}

	if !prepareLockOk {
		c.prepareState.Store(PrepareStateLockFail)
		return errn.ErrPrepareLockFail
	}

	c.hasPrepareLock.Store(true)
	c.prepareState.Store(PrepareStateLocked)

	c.server.txPrepareWg.Add(1)
	go func() {
		defer c.server.txPrepareWg.Done()
		select {
		case <-time.After(3 * time.Second):
			c.prepareState.Store(PrepareStateUnlock)
			releaseFunc()
			c.resetTx()
		case <-c.prepareUnlockSig:
			c.prepareState.Store(PrepareStateUnlock)
			<-c.queueCommandDone
			releaseFunc()

			c.resetTx()
			c.prepareUnlockDone <- struct{}{}
		}
	}()
	return nil
}

func execCommand(c *Client) (cerr error) {
	if !c.server.openDistributedTx {
		return errn.ErrTxDisable
	}
	if len(c.Args) != 0 {
		return resp.CmdParamsErr(resp.EXEC)
	}
	if c.txState&TxStatePrepare == 0 {
		return errn.ErrExecNotPrepared
	}
	prepareState := c.prepareState.Load()
	if prepareState == PrepareStateKeyModified || prepareState == PrepareStateLockFail {
		c.RespWriter.WriteBulk(nil)
		return nil
	}

	needUnlock := false
	releaseLock := func() {
		if cerr != nil {
			return
		}
		if needUnlock {
			c.queueCommandDone <- struct{}{}
			<-c.prepareUnlockDone
		} else {
			c.unwatchKey()
			c.resetTx()
		}
	}
	defer releaseLock()

	if len(c.commandQueue) == 0 {
		c.RespWriter.WriteStatus("(empty array)")
		return nil
	}

	if c.hasPrepareLock.Load() {
		if c.prepareState.Load() != PrepareStateLocked {
			cerr = errn.ErrPrepareLockTimeout
			return
		} else {
			needUnlock = true
			c.prepareUnlockSig <- struct{}{}
		}
	}

	c.disableCommandQueued()
	c.RespWriter.SetCached()
	for _, command := range c.commandQueue {
		c.HandleRequest(config.GlobalConfig.Plugin.OpenRaft, command, false)
	}
	c.RespWriter.UnsetCached()
	c.RespWriter.FlushCached()
	return nil
}

func discardCommand(c *Client) error {
	if !c.server.openDistributedTx {
		return errn.ErrTxDisable
	}
	if len(c.Args) != 0 {
		return resp.CmdParamsErr(resp.DISCARD)
	}
	if c.txState&TxStateMulti == 0 {
		return errn.ErrDiscardNoMulti
	}

	c.discard()
	c.RespWriter.WriteStatus("OK")
	return nil
}
