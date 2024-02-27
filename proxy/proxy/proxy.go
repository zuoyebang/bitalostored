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
	"net"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/internal/rpc"
	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
	_ "github.com/zuoyebang/bitalostored/proxy/respcmd"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"golang.org/x/net/netutil"
)

type Proxy struct {
	xauth       string
	online      atomic.Bool
	closed      atomic.Bool
	exitC       chan struct{}
	model       *models.Proxy
	config      *config.Config
	proxyClient *router.ProxyClient
	lproxy      net.Listener
	ladmin      net.Listener
}

func New(cfg *config.Config) (*Proxy, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	p := &Proxy{}
	p.closed.Store(false)
	p.online.Store(false)
	p.config = cfg
	p.exitC = make(chan struct{})
	pwd, _ := os.Getwd()
	p.model = &models.Proxy{
		StartTime:   time.Now().String(),
		ProductName: cfg.ProductName,
		CloudType:   cfg.ProxyCloudType,
		Pid:         os.Getpid(),
		Pwd:         pwd,
		VersionTag:  utils.GetVersionTag(),
		Hostname:    butils.Hostname,
	}

	if b, err := exec.Command("uname", "-a").Output(); err == nil {
		p.model.Sys = strings.TrimSpace(string(b))
	}

	if err := p.setup(cfg); err != nil {
		p.Close()
		return nil, err
	}

	p.proxyClient = router.NewProxyClient(cfg)

	go serveProxy(p, cfg)
	go serveAdmin(p)

	p.startProbeNode()
	p.startMetricsExporter(cfg)

	return p, nil
}

func (p *Proxy) ProxyAddress() string {
	return p.lproxy.Addr().String()
}

func (p *Proxy) AdminAddress() string {
	return p.ladmin.Addr().String()
}

func (p *Proxy) setup(config *config.Config) error {
	proto := config.ProtoType
	l, err := net.Listen(proto, config.ProxyAddr)
	if err != nil {
		return err
	}
	p.lproxy = netutil.LimitListener(l, config.ProxyMaxClients)
	proxyAddr, err := butils.ReplaceUnspecifiedIP(proto, p.ProxyAddress(), config.HostProxy)
	if err != nil {
		return err
	}
	p.model.ProtoType = proto
	p.model.ProxyAddr = proxyAddr
	p.model.HostPort = butils.GetLocalIp() + ":" + butils.GetPortByHostPort(config.ProxyAddr)

	proto = "tcp"
	p.ladmin, err = net.Listen(proto, config.AdminAddr)
	if err != nil {
		return err
	}
	adminAddr, err := butils.ReplaceUnspecifiedIP(proto, p.AdminAddress(), "")
	if err != nil {
		return err
	}
	p.model.AdminAddr = adminAddr
	p.model.Token = rpc.NewToken(
		config.ProductName,
		p.model.Hostname,
		p.model.HostPort,
	)
	p.xauth = rpc.NewXAuth(
		config.ProductName,
		config.ProductAuth,
		p.model.Token,
	)

	return nil
}

func (p *Proxy) Start() error {
	if p.closed.Load() {
		return errn.ErrClosedProxy
	}
	if p.online.Load() {
		return nil
	}
	p.online.Store(true)
	return nil
}

func (p *Proxy) Close() error {
	if p.closed.Load() {
		return nil
	}
	p.closed.Store(true)
	close(p.exitC)
	if p.lproxy != nil {
		p.lproxy.Close()
	}
	if p.ladmin != nil {
		p.ladmin.Close()
	}
	return nil
}

func (p *Proxy) XAuth() string {
	return p.xauth
}

func (p *Proxy) Model() *models.Proxy {
	return p.model
}

func (p *Proxy) Config() *config.Config {
	return p.config
}

func (p *Proxy) IsOnline() bool {
	return p.online.Load() && !p.closed.Load()
}

func (p *Proxy) IsClosed() bool {
	return p.closed.Load()
}

func (p *Proxy) startProbeNode() {
	go func() {
		doProbe := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("DoProbeNode panic err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			p.proxyClient.DoProbeNode()
		}

		log.Infof("probe node start working ...")

		duration := 2 * time.Second
		var ticker = time.NewTimer(duration)
		defer ticker.Stop()

		for !p.IsClosed() {
			<-ticker.C
			doProbe()
			ticker.Reset(duration)
		}
	}()
}
