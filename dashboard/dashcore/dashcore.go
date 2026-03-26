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

package dashcore

import (
	"container/list"
	"fmt"
	"github.com/zuoyebang/bitalostored/dashboard/internal/consts"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/rpc"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
	"github.com/zuoyebang/bitalostored/dashboard/internal/utils"
	"github.com/zuoyebang/bitalostored/dashboard/models"
	dbclient "github.com/zuoyebang/bitalostored/dashboard/models/db"

	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2/atomic2"
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
		dk      map[string]*models.DkItem
		migrate map[int]*models.Migrate
	}

	exit struct {
		C chan struct{}
	}

	config *Config
	online bool
	closed bool
	role   atomic.Value

	ladmin net.Listener

	stats struct {
		redisp *uredis.Pool

		servers map[string]*RedisStats
		proxies map[string]*ProxyStats
	}

	action struct {
		redisp *uredis.Pool

		disabled atomic2.Bool

		progress struct {
			status atomic.Value
		}
		executor atomic2.Int64
	}

	groupsyncStats   map[int][]error
	forceRefillCache atomic2.Int64
}
type AdminModel string

var RaftAdminModel AdminModel = "raft"

var ErrClosedDashCore = errors.New("use of closed dashcore")

func (s *DashCore) setRole(role string) {
	s.role.Store(role)
}

func (s *DashCore) getRole() string {
	if r, ok := s.role.Load().(string); ok {
		return r
	}
	return ""
}

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
		s.model.HostPort = utils.GetLocalIp() + ":" + utils.GetPortByHostPort(config.AdminAddr)
		s.model.AdminAddr = s.model.HostPort
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth(config.ProductName)
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
		s.action.redisp, s.stats.redisp,
	} {
		if p != nil {
			p.Close()
		}
	}

	defer s.store.Close()

	if s.online {
		if s.config.CoordinatorName == consts.DbTypeSqlite {
			if dbclient.IsAllowUpdate() {
				if err := s.store.Release(); err != nil {
					log.ErrorErrorf(err, "store: release lock of %s failed", s.config.ProductName)
					return errors.Errorf("store: release lock of %s failed", s.config.ProductName)
				}
			} else {
				if err := s.store.ReleaseBackUp(); err != nil {
					log.ErrorErrorf(err, "store: release backup lock of %s failed", s.config.ProductName)
					return errors.Errorf("store: release backup lock of %s failed", s.config.ProductName)
				}
			}
		}
		if s.config.CoordinatorName == consts.DbTypeMysql {
			topom, err := s.store.LoadDashCore()
			if err == nil {
				if topom == nil || topom.AddrCloud == nil {
					return nil
				}
				delete(topom.AddrCloud, s.model.HostPort)
				err = s.store.UpdateDashCore(topom)
				if err != nil {
					log.ErrorErrorf(err, "[%s] failed to update topom, removed on DH close [%s]", s.config.ProductName, s.model.HostPort)
				}
			} else {
				log.ErrorErrorf(err, "close get %s dashcore failed", s.config.ProductName)
			}
		}

	}
	return nil
}

func (s *DashCore) StartByDb(routines bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedDashCore
	}
	if s.online {
		return nil
	} else {
		getLock := false
		lockName := fmt.Sprintf("dashboard_start_dashcore_%s", s.config.ProductName)
		if dbclient.IsDBInit() {
			dbclient.ChangeAllowUpdateStatus(false)
			for i := 0; i < 3; i++ {
				if err := dbclient.AddLock(lockName); err == nil {
					getLock = true
					break
				} else {
					log.Warn("lock dashcore failed.", err)
				}
				time.Sleep(time.Second)
			}
			dashCoreExist := true
			masterDashCore, masterErr := s.store.LoadDashCore()
			if masterErr != nil {
				log.ErrorErrorf(masterErr, "get %s dashcore failed", s.config.ProductName)
				dashCoreExist = false
			}
			if masterDashCore == nil || masterDashCore.HostPort == "" {
				dashCoreExist = false
			}
			backupDashCore, backupErr := s.store.LoadBackUpDashCore()
			if backupErr != nil {
				log.ErrorErrorf(backupErr, "get %s backup dashcore failed", s.config.ProductName)
			}

			matchedBackup := false
			if backupDashCore != nil && backupDashCore.HostPort == s.model.HostPort {
				matchedBackup = true
			}
			if backupDashCore != nil {
				s.model.BackupAddr = backupDashCore.AdminAddr
				s.model.BackupHostPort = backupDashCore.HostPort
			}

			if getLock && ((!dashCoreExist && !matchedBackup) || (masterDashCore != nil && masterDashCore.HostPort == s.model.HostPort)) {
				log.Info("start dashboard as role master")
				if masterDashCore == nil {
					if err := s.store.Acquire(s.model); err != nil {
						log.ErrorErrorf(err, "store: acquire lock of %s failed", s.config.ProductName)
						return errors.Errorf("store: acquire lock of %s failed", s.config.ProductName)
					}
				}
				dbclient.ChangeAllowUpdateStatus(true)
			} else {
				log.Info("start dashboard as role backup")
				if masterDashCore != nil {
					s.model.BackupAddr = masterDashCore.AdminAddr
					s.model.BackupHostPort = masterDashCore.HostPort
				}
				err := s.store.BackUp(s.model)
				if err != nil && strings.Contains(err.Error(), "record exists.full_path:") {
					s.store.ReleaseBackUp()
					err = s.store.BackUp(s.model)
				}
				if err != nil {
					log.ErrorErrorf(err, "store: backup dashcore of %s failed", s.config.ProductName)
					return errors.Errorf("store: backup dashcore of %s failed", s.config.ProductName)
				}
			}
			if getLock {
				if err := dbclient.DeleteLock(lockName); err != nil {
					log.ErrorErrorf(err, "unlock the %s dashcore failed", s.config.ProductName)
				}
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

	if s.config.Env != consts.EnvDemo {
		go func() {
			for !s.IsClosed() {
				if s.IsOnline() {
					role := false
					masterDashCore, masterErr := s.store.LoadDashCore()
					if masterErr != nil {
						log.ErrorErrorf(masterErr, "get %s dashcore failed", s.config.ProductName)
					} else {
						if masterDashCore != nil && masterDashCore.HostPort == s.model.HostPort {
							role = true
						}
						if !dbclient.IsAllowUpdate() && role {
							log.Info("backup dashboard refresh cache.")
							s.dirtyCacheAll()
							s.refillCache()
						}
						dbclient.ChangeAllowUpdateStatus(role)
					}
					backupDashCore, backupErr := s.store.LoadBackUpDashCore()
					if backupErr != nil {
						log.ErrorErrorf(backupErr, "get %s backupdashcore failed", s.config.ProductName)
					} else {
						if backupDashCore == nil {
							s.model.BackupHostPort = ""
							s.model.BackupAddr = ""
						} else {
							s.model.BackupHostPort = backupDashCore.HostPort
							s.model.BackupAddr = backupDashCore.AdminAddr
						}
					}
				}
				time.Sleep(3 * time.Second)
			}
		}()
	}

	s.crontabCheckMasterByRaft()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if !dbclient.IsAllowUpdate() {
					time.Sleep(time.Second * 5)
					continue
				}
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
				if !dbclient.IsAllowUpdate() {
					time.Sleep(time.Second * 5)
					continue
				}
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	modelCopy := *s.model
	return &modelCopy
}

func formatCpuSetMsg(cpuSetType int) string {
	var res string
	if cpuSetType == 0 {
		res = "CPU not bound"
	}
	if cpuSetType == 1 {
		res = "CPU exclusive"
	}
	if cpuSetType == 2 {
		res = "CPU shared"
	}
	return res
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
			ctx.dk = s.cache.dk
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
	stats.Group.Servers = make(map[int]map[string]*RedisStats, len(s.cache.group)*5)
	for _, g := range s.cache.group {
		for _, x := range g.Servers {
			lk := utils.ServerGroupKey(x.Addr, g.Id)
			if _, ok := stats.Group.Servers[g.Id]; !ok {
				stats.Group.Servers[g.Id] = make(map[string]*RedisStats, len(g.Servers))
				stats.Group.Servers[g.Id][x.Addr] = s.stats.servers[lk]
			} else {
				stats.Group.Servers[g.Id][x.Addr] = s.stats.servers[lk]
			}
			if v := s.stats.servers[lk]; v != nil {
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
		Models  []*models.Group                `json:"models"`
		Stats   map[string]*RedisStats         `json:"stats"`
		Servers map[int]map[string]*RedisStats `json:"server_stats"`
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.online && !s.closed
}

func (s *DashCore) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *DashCore) SetSlotActionDisabled(value bool) {
	s.action.disabled.Set(value)
	log.Warnf("set action disabled = %t", value)
}

func (s *DashCore) TransferSlots(sid, eid, toGid int) error {
	// check slot range [sid-eid] has the same group
	err := s.sendTransferSlots(sid, eid, toGid)
	if err != nil {
		return err
	}
	slots := make([]*models.SlotMapping, 0, eid-sid)
	for i := sid; i <= eid; i++ {
		slots = append(slots, &models.SlotMapping{Id: i, GroupId: toGid})
	}
	return s.SlotsAssignGroup(slots)
}

func (s *DashCore) RemoveSlotData(sid, eid, gid int, token string) error {
	return s.sendRemoveSlots(sid, eid, gid, token)
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
	s.mu.RLock()
	pn := s.model.ProductName
	defer s.mu.RUnlock()
	return s.store.UpdateDepartment(newDepartment, pn)
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

func (s *DashCore) GetSlotActionHistory() ([]*dbclient.SlotAction, error) {
	cluster := s.model.ProductName
	res, err := dbclient.SlotActionGetList(cluster)
	if err != nil {
		log.Infof("get slot history. err:%s", err)
		return nil, nil
	}
	return res, nil
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
	Version     string           `json:"version"`
	Compile     string           `json:"compile"`
	Role        string           `json:"role"`
	Master      string           `json:"master"`
	Backup      []string         `json:"backup"`
	Config      *Config          `json:"config,omitempty"`
	Model       *models.DashCore `json:"model,omitempty"`
	Stats       *Stats           `json:"stats,omitempty"`
	Cgroup      string           `json:"cgroup,omitempty"`
	MonitorLink *MonitorLink     `json:"monitorLink,omitempty"`
}

type MonitorLink struct {
	Tx  string `json:"tx"`
	ALi string `json:"ali"`
	Bd  string `json:"bd"`
}

func (s *DashCore) Overview() (*Overview, error) {
	if stats, err := s.Stats(); err != nil {
		return nil, err
	} else {
		rt := &Overview{
			Version: utils.Version,
			Compile: utils.Compile,
			Role:    s.getRole(),
			Config:  s.Config(),
			Model:   s.Model(),
			Stats:   stats,
			Backup:  make([]string, 0),
		}
		if s.forceRefillCache.AsInt()%10 == 0 {
			topom, err := s.store.LoadDashCore()
			if err == nil {
				s.mu.Lock()
				s.model.AddrCloud = topom.AddrCloud
				s.mu.Unlock()
			}
		}
		if s.config.Env != consts.EnvDemo {
			rt.Cgroup = formatCgroup(s.model.ProductName, stats)
			rt.MonitorLink = getOdinLink(s.model.ProductName)
			if s.getRole() == consts.DhRoleMaster {
				rt.Master = s.Model().HostPort
				for addr, _ := range s.Model().AddrCloud {
					if addr != s.Model().HostPort {
						rt.Backup = append(rt.Backup, addr)
					}
				}
			}
		}
		rt.Config.Database = DBConfig{}
		return rt, nil
	}
}

func getOdinLink(productName string) (m *MonitorLink) {
	cluster, err := dbclient.GetClusterByClusterName(productName, 2)
	if err != nil {
		return
	}
	if len(cluster.MonitorLink) > 0 {
		err := jsoniter.UnmarshalFromString(cluster.MonitorLink, &m)
		if err != nil {
			return
		}
	}
	return
}

func formatCgroup(productName string, stat *Stats) string {
	var ret string
	resourceList, err := dbclient.GetCgroupByClusterName(productName)
	var proxyExclusive, serverExclusive int16
	if err == nil {
		var proxy, server strings.Builder
		proxy.WriteString("proxy: ")
		server.WriteString("server: ")
		for _, resource := range resourceList {
			if resource.ServiceId == 2 {
				proxyTmp := fmt.Sprintf("%s-%d-%s-%d;", resource.IDC, resource.Port, formatCpuSetMsg(resource.CpuSetType), resource.CgroupLimit)
				proxy.WriteString(proxyTmp)
				proxyExclusive = resource.CgroupLimit
			}
			if resource.ServiceId == 6 {
				serverTmp := fmt.Sprintf("%s-%s-%d;", resource.IDC, formatCpuSetMsg(resource.CpuSetType), resource.CgroupLimit)
				server.WriteString(serverTmp)
				serverExclusive = resource.CgroupLimit
			}
		}
		var s, ps, p int16
		if stat.Group.Models != nil {
			for _, model := range stat.Group.Models {
				for _, server := range model.Servers {
					if server.ServerRole == models.ServerMasterSlaveNode {
						s += 1
					}
				}
			}
		}
		if stat.Proxy.Models != nil {
			var port string
			if productName == "ocr-search" {
				port = "8146"
			}
			if productName == "ocr-search-inv" {
				port = "8026"
			}
			if productName == "ocr-search-page" {
				port = "8016"
			}
			if len(port) > 0 {
				for _, m := range stat.Proxy.Models {
					if strings.Contains(m.HostPort, port) {
						ps += 1
					} else {
						p += 1
					}
				}
			} else if proxyExclusive == 0 {
				ps = int16(len(stat.Proxy.Models))
			} else {
				p = int16(len(stat.Proxy.Models))
			}
		}
		proxyExclusive = p * proxyExclusive
		serverExclusive = s * serverExclusive
		ret = fmt.Sprintf("%s<br>%s<br>proxy cores: %d<br>server cores: %d", proxy.String(), server.String(), proxyExclusive, serverExclusive)
	}
	return ret
}
