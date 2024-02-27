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

package dashcore

import (
	"container/list"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/rpc"
	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2/atomic2"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
	"github.com/zuoyebang/bitalostored/dashboard/internal/utils"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

type DashCore struct {
	mu sync.RWMutex

	xauth      string
	adminModel AdminModel
	model      *models.DashCore
	store      *models.Store
	cache      struct {
		hooks   list.List
		slots   []*models.SlotMapping
		group   map[int]*models.Group
		proxy   map[string]*models.Proxy
		pconfig map[string]*models.Pconfig
		migrate map[int]*models.Migrate
	}

	exit struct {
		C chan struct{}
	}

	config *Config
	online bool
	closed bool

	ladmin net.Listener

	stats struct {
		redisp *uredis.Pool

		servers map[string]*RedisStats
		proxies map[string]*ProxyStats
	}

	action struct {
		redisp   *uredis.Pool
		disabled atomic2.Bool

		progress struct {
			status atomic.Value
		}
		executor atomic2.Int64
	}

	ha struct {
		redisp  *uredis.Pool
		masters map[int]string
	}

	groupsyncStats   map[int][]error
	forceRefillCache atomic2.Int64
}
type AdminModel string

var RaftAdminModel AdminModel = "raft"

var ErrClosedDashCore = errors.New("use of closed dashcore")

func New(client models.Client, config *Config) (*DashCore, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}
	s := &DashCore{}
	s.config = config
	s.exit.C = make(chan struct{})
	s.groupsyncStats = make(map[int][]error)
	s.ha.redisp = uredis.NewPool("", time.Minute*10)

	s.action.redisp = uredis.NewPool(config.ProductAuth, time.Minute*10)
	s.action.progress.status.Store("")

	readCrossCloud := true
	if config.ReadCrossCloud == -1 {
		readCrossCloud = false
	}

	s.model = &models.DashCore{
		ReadCrossCloud: readCrossCloud,
		StartTime:      time.Now().String(),
	}
	s.adminModel = AdminModel(config.AdminModel)
	s.model.ProductName = config.ProductName
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}
	s.store = models.NewStore(client, config.ProductName)

	s.stats.redisp = uredis.NewPool(config.ProductAuth, time.Minute*10)
	s.stats.servers = make(map[string]*RedisStats)
	s.stats.proxies = make(map[string]*ProxyStats)

	if err := s.setup(config); err != nil {
		s.Close()
		return nil, err
	}

	log.Warnf("create new dashcore:\n%s", s.model.Encode())

	go s.serveAdmin()

	return s, nil
}

func (s *DashCore) setup(config *Config) error {
	if l, err := net.Listen("tcp", config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := butils.ReplaceUnspecifiedIP("tcp", l.Addr().String(), s.config.HostAdmin)
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
		s.model.HostPort = butils.GetLocalIp() + ":" + butils.GetPortByHostPort(config.AdminAddr)
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth(config.ProductName)
	fmt.Println(s.xauth)
	return nil
}

func (s *DashCore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.ladmin != nil {
		s.ladmin.Close()
	}
	for _, p := range []*uredis.Pool{
		s.action.redisp, s.stats.redisp, s.ha.redisp,
	} {
		if p != nil {
			p.Close()
		}
	}

	defer s.store.Close()

	if s.online {
		if err := s.store.Release(); err != nil {
			log.ErrorErrorf(err, "store: release lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: release lock of %s failed", s.config.ProductName)
		}
	}
	return nil
}

func (s *DashCore) Start(routines bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedDashCore
	}
	if s.online {
		return nil
	} else {
		masterTopom, _ := s.store.LoadDashCore()
		if masterTopom == nil {
			if err := s.store.Acquire(s.model); err != nil {
				log.ErrorErrorf(err, "store: acquire lock of %s failed", s.config.ProductName)
				return errors.Errorf("store: acquire lock of %s failed", s.config.ProductName)
			}
		}
		s.online = true
	}

	if !routines {
		return nil
	}
	_, err := s.newContext()
	if err != nil {
		return err
	}

	go func() {
		s.InitDefaultPconfig()
	}()

	s.crontabCheckMasterByRaft()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshRedisStats(time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second * 5)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshProxyStats(time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second * 5)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSlotAction(); err != nil {
					log.WarnErrorf(err, "process slot action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
		}
	}()
	return nil
}

func (s *DashCore) XAuth() string {
	return s.xauth
}

func (s *DashCore) Model() *models.DashCore {
	return s.model
}

func (s *DashCore) AdminModel() AdminModel {
	return s.adminModel
}

var ErrNotOnline = errors.New("DashCore is not online")

func (s *DashCore) newContext() (*context, error) {
	if s.closed {
		return nil, ErrClosedDashCore
	}
	if s.online {
		if err := s.refillCache(); err != nil {
			return nil, err
		} else {
			ctx := &context{}
			ctx.slots = s.cache.slots
			ctx.group = s.cache.group
			ctx.proxy = s.cache.proxy
			ctx.migrate = s.cache.migrate
			ctx.pconfig = s.cache.pconfig
			ctx.hosts.m = make(map[string]net.IP)
			return ctx, nil
		}
	} else {
		return nil, ErrNotOnline
	}
}

func (s *DashCore) Stats() (*Stats, error) {
	s.forceRefillCache.Add(1)
	if s.forceRefillCache.AsInt()%50 == 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.refillCache()
	} else {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}

	stats := &Stats{}
	stats.Closed = s.closed
	stats.ReadCrossCloud = s.model.ReadCrossCloud

	stats.Slots = s.cache.slots

	stats.Group.Models = models.SortGroup(s.cache.group)
	stats.Group.Stats = map[string]*RedisStats{}
	for _, g := range s.cache.group {
		for _, x := range g.Servers {
			if v := s.stats.servers[x.Addr]; v != nil {
				stats.Group.Stats[x.Addr] = v
			}
		}
	}

	stats.Proxy.Models = models.SortProxy(s.cache.proxy)
	stats.Proxy.Stats = s.stats.proxies

	stats.SlotAction.Disabled = s.action.disabled.Bool()
	stats.SlotAction.Progress.Status = s.action.progress.status.Load().(string)
	stats.SlotAction.Executor = s.action.executor.Int64()

	stats.GroupSyncStats = make([]string, 0, 2)
	for gid, errs := range s.groupsyncStats {
		for _, err := range errs {
			if err != nil {
				stats.GroupSyncStats = append(stats.GroupSyncStats, fmt.Sprintf("sync groupid:%d ", gid)+err.Error())
			}
		}
	}
	return stats, nil
}

type Stats struct {
	Closed bool `json:"closed"`

	ReadCrossCloud bool `json:"read_cross_cloud"`

	Slots []*models.SlotMapping `json:"slots"`

	Group struct {
		Models []*models.Group        `json:"models"`
		Stats  map[string]*RedisStats `json:"stats"`
	} `json:"group"`

	Proxy struct {
		Models []*models.Proxy        `json:"models"`
		Stats  map[string]*ProxyStats `json:"stats"`
	} `json:"proxy"`

	SlotAction struct {
		Disabled bool `json:"disabled"`

		Progress struct {
			Status string `json:"status"`
		} `json:"progress"`

		Executor int64 `json:"executor"`
	} `json:"slot_action"`

	GroupSyncStats []string `json:"group_sync_stats"`
}

func (s *DashCore) Config() *Config {
	return s.config
}

func (s *DashCore) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *DashCore) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *DashCore) SetSlotActionDisabled(value bool) {
	s.action.disabled.Set(value)
	log.Warnf("set action disabled = %t", value)
}

func (s *DashCore) Slots() ([]*models.Slot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	return ctx.toSlotSlice(ctx.slots, nil), nil
}

func (s *DashCore) UpdateDepartment(newDepartment string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.UpdateDepartment(newDepartment, s.model.ProductName)
}

func (s *DashCore) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.newContext()
	if err != nil {
		return err
	}
	defer s.dirtyCacheAll()
	return nil
}

func (s *DashCore) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("admin start service on %s", s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("admin shutdown")
	case err := <-eh:
		log.ErrorErrorf(err, "admin exit on error")
	}
}

type Overview struct {
	Version string           `json:"version"`
	Compile string           `json:"compile"`
	Config  *Config          `json:"config,omitempty"`
	Model   *models.DashCore `json:"model,omitempty"`
	Stats   *Stats           `json:"stats,omitempty"`
}

func (s *DashCore) Overview() (*Overview, error) {
	if stats, err := s.Stats(); err != nil {
		return nil, err
	} else {
		return &Overview{
			Version: utils.Version,
			Compile: utils.Compile,
			Config:  s.Config(),
			Model:   s.Model(),
			Stats:   stats,
		}, nil
	}
}
