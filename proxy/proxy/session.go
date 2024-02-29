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

package proxy

import (
	"bytes"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/cockroachdb/errors"
)

var errClientQuit = errors.New("remote client quit")

var doOnce sync.Once
var globalRequestClient *requestClient

type requestClient struct {
	mutex         sync.Mutex
	proxyConnWait sync.WaitGroup
	rcs           map[*sessionClient]struct{}
}

type sessionClient struct {
	sproxy     *Proxy
	accessLog  bool
	slowLog    bool
	slowCost   int64
	remoteAddr string
	session    *resp.Session
	conn       net.Conn
	buf        bytes.Buffer
	rqc        *requestClient
}

func newGlobalRequestClient() *requestClient {
	doOnce.Do(func() {
		globalRequestClient = &requestClient{
			mutex:         sync.Mutex{},
			proxyConnWait: sync.WaitGroup{},
			rcs:           make(map[*sessionClient]struct{}, 128),
		}
	})
	return globalRequestClient
}

func newClientRESP(conn net.Conn, p *Proxy, cfg *config.Config) {
	c := &sessionClient{}
	c.rqc = newGlobalRequestClient()
	c.session = resp.NewSession(
		conn,
		cfg.ConnReadBufferSize.AsInt(),
		cfg.ConnWriteBufferSize.AsInt(),
		cfg.OpenDistributedTx)
	c.sproxy = p
	c.remoteAddr = conn.RemoteAddr().String()
	c.accessLog = cfg.Log.AccessLog
	c.slowLog = cfg.Log.SlowLog
	c.slowCost = cfg.Log.SlowLogCost.Int64()
	c.session.SetAuth(cfg.ProxyAuthEnabled, cfg.ProxyAuthPassword, cfg.ProxyAuthAdmin)
	c.session.SetLastQueryTime()
	c.buf = bytes.Buffer{}
	c.rqc.proxyConnWait.Add(1)
	c.rqc.addRespClient(c)
	go c.run()
}

func (sc *sessionClient) Close() {
	sc.rqc.delRespClient(sc)
	sc.rqc.proxyConnWait.Done()
	sc.session.Close()
}

func (rc *requestClient) addRespClient(c *sessionClient) {
	rc.mutex.Lock()
	rc.rcs[c] = struct{}{}
	rc.mutex.Unlock()
}

func (rc *requestClient) delRespClient(c *sessionClient) {
	rc.mutex.Lock()
	delete(rc.rcs, c)
	rc.mutex.Unlock()
}

func (rc *requestClient) closeAllRespClients() {
	rc.mutex.Lock()
	for c := range rc.rcs {
		c.session.Close()
	}
	rc.mutex.Unlock()
}

func (rc *requestClient) respClientNum() int {
	rc.mutex.Lock()
	n := len(rc.rcs)
	rc.mutex.Unlock()
	return n
}

func (sc *sessionClient) run() {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			log.Errorf("client run panic err:%v stack:%s", e, unsafe2.String(buf[:n]))
		}
		sc.Close()
	}()

	select {
	case <-sc.sproxy.exitC:
		return
	default:
		break
	}

	for {
		sc.session.SetReadDeadline()
		sc.session.Cmd = ""
		sc.session.Args = nil
		sc.session.SetLastQueryTime()
		reqData, err := sc.session.RespReader.ParseRequest()
		if err != nil {
			return
		}

		sc.session.SetQueryProperty(true)
		start := time.Now()
		if err = sc.handleRequest(reqData); err != nil {
			argsTmp := make([]string, 0, len(sc.session.Args))
			for _, args := range sc.session.Args {
				argsTmp = append(argsTmp, unsafe2.String(args))
			}
			log.Warnf("handleRequest romoteAddr:%s cmd:%s args:%v err:%s", sc.remoteAddr, sc.session.Cmd, argsTmp, err.Error())
		}

		if sc.accessLog {
			duration := time.Since(start)
			fullCmd := sc.catGenericCommand()
			cost := duration.Nanoseconds() / 1000
			truncateLen := len(fullCmd)
			if truncateLen > 256 {
				truncateLen = 256
			}
			log.Access(sc.remoteAddr, cost, fullCmd[:truncateLen], err)
		}

		if sc.slowLog {
			duration := time.Since(start)
			cost := duration.Nanoseconds()
			if cost >= sc.slowCost {
				fullCmd := sc.catGenericCommand()
				truncateLen := len(fullCmd)
				if truncateLen > 256 {
					truncateLen = 256
				}
				cost = cost / 1000
				log.Slow(sc.remoteAddr, cost, fullCmd[:truncateLen], err)
			}
		}

		sc.session.RespWriter.Flush()
		sc.session.SetQueryProperty(false)
	}
}

func (sc *sessionClient) catGenericCommand() []byte {
	buffer := sc.buf
	buffer.Reset()
	buffer.Write([]byte(sc.session.Cmd))
	for _, arg := range sc.session.Args {
		buffer.WriteByte(' ')
		buffer.Write(arg)
	}

	return buffer.Bytes()
}

func (sc *sessionClient) handleRequest(reqData [][]byte) error {
	if len(reqData) == 0 {
		sc.session.Cmd = ""
		sc.session.Args = reqData[0:0]
	} else {
		sc.session.Cmd = unsafe2.String(resp.UpperSlice(reqData[0]))
		sc.session.Args = reqData[1:]
	}

	if len(sc.session.Args) >= 1 {
		key := unsafe2.String(sc.session.Args[0])
		if CheckIsBlackKey(key) {
			if !CheckIsWhiteKey(key) {
				sc.session.RespWriter.WriteError(errors.Errorf("black key: %s", key))
				return nil
			}
		}
	}

	if sc.session.Cmd == "INFO" {
		sc.session.RespWriter.WriteBulk(proxyInfo)
		return nil
	}

	if sc.session.Cmd == "QUIT" {
		sc.session.RespWriter.WriteStatus(resp.ReplyOK)
		return errClientQuit
	}

	startUninNano := time.Now().UnixNano()

	return sc.session.Perform(startUninNano)
}
