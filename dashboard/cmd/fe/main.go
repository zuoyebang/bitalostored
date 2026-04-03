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

package main

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/dashboard/dashcore"
	"github.com/zuoyebang/bitalostored/dashboard/internal/consts"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
	dbclient "github.com/zuoyebang/bitalostored/dashboard/models/db"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"gorm.io/driver/sqlite"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"github.com/spf13/pflag"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/rpc"
	"github.com/zuoyebang/bitalostored/dashboard/internal/sync2/atomic2"
	"github.com/zuoyebang/bitalostored/dashboard/internal/utils"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

var roundTripper http.RoundTripper

func init() {
	var dials atomic2.Int64
	tr := &http.Transport{}
	tr.Dial = func(network, addr string) (net.Conn, error) {
		c, err := net.DialTimeout(network, addr, time.Second*10)
		if err == nil {
			log.Debugf("rpc: dial new connection to [%d] %s - %s",
				dials.Incr()-1, network, addr)
		}
		return c, err
	}
	go func() {
		for {
			time.Sleep(time.Minute)
			tr.CloseIdleConnections()
		}
	}()
	roundTripper = tr
}

func main() {
	configPath := pflag.String("config", "", "run with the specific configuration")
	version := pflag.Bool("version", false, "get current version")
	logPath := pflag.String("log", "", "set path/name of daliy rotated log file.")
	logLevel := pflag.String("log-level", "INFO", "set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.")
	listen := pflag.String("listen", "", "set the listen address.")
	assetsDir := pflag.String("assets-dir", "", "set path of assets directory.")
	pidFile := pflag.String("pidfile", "", "set the pid file.")
	pflag.Parse()
	cfg := dashcore.NewDefaultFEConfig()
	if *configPath != "" {
		if err := cfg.LoadFromFile(*configPath); err != nil {
			panic(fmt.Sprintf("load config failed err:%s", err.Error()))
		}
		log.Infof("load config %s", cfg.String())
	}

	if *version {
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return
	}
	if *logPath != "" {
		w, err := log.NewRollingFile(*logPath, log.HourlyRolling)
		if err != nil {
			log.PanicErrorf(err, "open log file %s failed", *logPath)
		} else {
			log.StdLog = log.New(w, "")
		}
	}

	if *logLevel != "" {
		if !log.SetLevelString(*logLevel) {
			log.Panicf("option --log-level = %s", *logLevel)
		}
	} else {
		log.SetLevel(log.LevelInfo)
	}

	runtime.GOMAXPROCS(cfg.Ncpu)
	log.Infof("set ncpu = %d", runtime.GOMAXPROCS(0))
	log.Infof("set listen = %s", *listen)

	var assets string
	if *assetsDir != "" {
		abspath, err := filepath.Abs(*assetsDir)
		if err != nil {
			log.PanicErrorf(err, "get absolute path of %s failed", *assetsDir)
		}
		assets = abspath
	} else {
		binpath, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			log.PanicErrorf(err, "get path of binary failed")
		}
		assets = filepath.Join(binpath, "assets")
	}
	log.Infof("set assets = %s", assets)

	indexFile := filepath.Join(assets, "index.html")
	if _, err := os.Stat(indexFile); err != nil {
		log.PanicErrorf(err, "get stat of %s failed", indexFile)
	}

	var loader ConfigLoader
	var coordinator struct {
		name string
		addr string
		auth string
	}

	var db *gorm.DB
	var err error
	switch cfg.DbType {
	case consts.DbTypeMysql:
		coordinator.name = consts.DbTypeMysql
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			cfg.Database.Username, cfg.Database.Password, cfg.Database.HostPort, cfg.Database.DBName)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			log.PanicErrorf(err, "connect db failed.")
		}
	case consts.DbTypeSqlite:
		coordinator.name = consts.DbTypeSqlite
		coordinator.addr = cfg.Sqlite
		db, err = gorm.Open(sqlite.Open(coordinator.addr), &gorm.Config{})
		if err != nil {
			log.PanicErrorf(err, "connect sqlite failed.%+v", err)
		}
		log.Warnf("option --sqlite = %s", coordinator.addr)
	default:
		log.Panicf("invalid coordinator")
	}
	log.Infof("set --%s = %s", coordinator.name, coordinator.addr)

	c, err := models.NewClient(coordinator.name, db)
	if err != nil {
		log.PanicErrorf(err, "create '%s' client to '%s' failed", coordinator.name, coordinator.addr)
	}
	defer c.Close()

	loader = &DynamicLoader{c, nil}
	if coordinator.name == consts.DbTypeMysql {
		dbclient.ChangeAllowUpdateStatus(true)
		directOperator.Client = c
		directOperator.mu = &sync.Mutex{}
	} else {
		directOperator.Client = nil
		directOperator.mu = nil
	}

	router := NewReverseProxy(loader)

	m := martini.New()
	m.Use(martini.Recovery())
	m.Use(render.Renderer())
	m.Use(martini.Static(assets, martini.StaticOptions{SkipLogging: true}))

	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			log.Errorf("fe run [err:%v] [panic:%s]", e, string(buf))
		}
	}()

	router.GetNames()

	r := martini.NewRouter()
	r.Get("/ready", func() (int, string) {
		return 200, "success"
	})
	r.Get("/list", func() (int, string) {
		names := router.GetNames()
		sort.Sort(sort.StringSlice(names))
		return rpc.ApiResponseJson(names)
	})

	r.Get("/clusters", func() (int, string) {
		names := router.GetClusters()
		return rpc.ApiResponseJson(names)
	})

	r.Get("/constants", func() (int, string) {
		constant := map[string]interface{}{
			"clouds": []string{"tencent", "txcloud", "txsh", "txgz", "ali", "baidu"},
		}
		return rpc.ApiResponseJson(constant)
	})

	r.Post("/logquery", func(w http.ResponseWriter, req *http.Request) {
		name := req.URL.Query().Get("forward")
		if len(name) == 0 {
			return
		}
		port := 8080
		hostPort := fmt.Sprintf("%s:%d", name, port)
		director := func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = hostPort
			req.URL.Path = "/bitalosagent/logquery"
		}
		p := &httputil.ReverseProxy{Director: director}
		p.ServeHTTP(w, req)
	})

	r.Get("/proxy", func(w http.ResponseWriter, req *http.Request) {
		ha := req.URL.Query().Get("ha")
		if len(ha) == 0 {
			return
		}
		director := func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = ha
		}
		p := &httputil.ReverseProxy{Director: director}
		p.ServeHTTP(w, req)
	})

	r.Get("/proxy/stats", func(w http.ResponseWriter, req *http.Request) {
		ha := req.URL.Query().Get("ha")
		if len(ha) == 0 {
			return
		}
		director := func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = ha
		}
		p := &httputil.ReverseProxy{Director: director}
		p.ServeHTTP(w, req)
	})

	r.Get("/topomdirect", func(w http.ResponseWriter, req *http.Request) {
		ha := req.URL.Query().Get("ha")
		if len(ha) == 0 {
			return
		}
		director := func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = ha
			req.URL.Path = "/topom"
		}
		p := &httputil.ReverseProxy{Director: director}
		p.ServeHTTP(w, req)
	})

	r.Any("/**", func(w http.ResponseWriter, req *http.Request) {
		path := req.URL.Path
		name := req.URL.Query().Get("forward")
		if len(name) == 0 {
			if strings.Contains(path, "/admin") || strings.HasPrefix(path, "/login") || strings.HasPrefix(path, "/logout") {
				names := router.GetNames()
				sort.Sort(sort.StringSlice(names))
				if len(names) > 0 {
					name = names[0]
				}
			}
		}
		if p := router.GetProxy(name); p != nil {
			p.ServeHTTP(w, req)
		} else {
			w.WriteHeader(http.StatusForbidden)
		}
	})

	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.PanicErrorf(err, "listen %s failed", *listen)
	}
	defer l.Close()

	if *pidFile != "" {
		if pidfile, err := filepath.Abs(*pidFile); err != nil {
			log.WarnErrorf(err, "parse pidfile = '%s' failed", *pidFile)
		} else if err := os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
		} else {
			defer func() {
				if err := os.Remove(pidfile); err != nil {
					log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
				}
			}()
			log.Infof("option --pidfile = %s", pidfile)
		}
	}

	exitC := make(chan struct{}, 1)
	go serveAdmin(l, m, exitC)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

	sig := <-sc
	log.Warnf("fe receive signal = '%v'", sig)
	close(exitC)
}

func serveAdmin(l net.Listener, m *martini.Martini, exitC chan struct{}) {
	eh := make(chan error, 1)
	go func(l net.Listener, m *martini.Martini, eh chan error) {
		h := http.NewServeMux()
		h.Handle("/", m)
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(l, m, eh)

	select {
	case <-exitC:
		log.Warnf("admin shutdown")
	case err := <-eh:
		log.PanicError(err, "serve failed")
	}
}

var directOperator DBOperator

type DBOperator struct {
	mu     *sync.Mutex
	Client models.Client
}

type ConfigLoader interface {
	Reload() (map[string]string, error, map[string]string)
}

type DynamicLoader struct {
	client           models.Client
	storedConfigPool *uredis.StoredPool
}

func (l *DynamicLoader) Reload() (map[string]string, error, map[string]string) {
	var m = make(map[string]string)
	var department = make(map[string]string)

	departmentLists, err := l.client.SubListGroup("department", "product_name")
	if err != nil {
		return nil, errors.Trace(err), nil
	}
	departmentList := departmentLists.([]*dbclient.TblDashboard)
	for _, dm := range departmentList {
		var de = &models.Department{}
		if err := jsoniter.UnmarshalFromString(dm.Value, de); err != nil {
			log.WarnErrorf(err, "decode json failed")
			department[dm.ClusterName] = ""
			continue
		}
		department[dm.ClusterName] = de.Name
	}

	topomLists, err := l.client.SubListGroup("topom", "product_name")
	if err != nil {
		return nil, errors.Trace(err), nil
	}
	topomList := topomLists.([]*dbclient.TblDashboard)
	for _, dh := range topomList {
		var t = &models.DashCore{}
		if err := jsoniter.UnmarshalFromString(dh.Value, t); err != nil {
			log.WarnErrorf(err, "decode json failed")
		}
		if _, ok := department[t.ProductName]; !ok {
			department[t.ProductName] = ""
		}
		m[t.ProductName] = t.AdminAddr
	}
	return m, nil, department
}

type ReverseProxy struct {
	sync.RWMutex
	loader  ConfigLoader
	routes  map[string]*httputil.ReverseProxy
	details DetailsInfo

	clusterDepartment map[string]string
}
type DetailsInfo struct {
	Total    []TotalGroupInfo       `json:"total"`
	Clusters map[string]ClusterInfo `json:"clusters"`
}

type TotalGroupInfo struct {
	Name   string   `json:"name"`
	Count  int      `json:"count"`
	Detail []string `json:"detail"`
}

type ClusterInfo struct {
	Name              string   `json:"name"`
	GroupSum          int      `json:"group_sum"`
	Groups            []string `json:"groups"`
	GroupOutOfSyncSum int      `json:"group_out_of_sync_sum"`
	GroupOutOfSync    []string `json:"group_out_of_sync"`    //groupId array
	GroupDegrade      []string `json:"group_degrade"`        //groupId array
	GroupUpdateOneDay []string `json:"group_update_one_day"` //groupId array
}

func NewReverseProxy(loader ConfigLoader) *ReverseProxy {
	r := &ReverseProxy{}
	r.loader = loader
	r.routes = make(map[string]*httputil.ReverseProxy)

	go func() {
		for {
			r.reload()
			time.Sleep(5 * time.Second)
		}
	}()

	return r
}

func (r *ReverseProxy) reload() {
	if m, err, department := r.loader.Reload(); err != nil {
		log.WarnErrorf(err, "reload reverse proxy failed")
	} else {
		r.Lock()
		defer r.Unlock()
		log.Info("reverse proxy routes reloaded successfully")
		r.routes = make(map[string]*httputil.ReverseProxy)
		for name, host := range m {
			if name == "" || host == "" {
				continue
			}
			u := &url.URL{Scheme: "http", Host: host}
			p := httputil.NewSingleHostReverseProxy(u)
			p.Transport = roundTripper
			r.routes[name] = p
		}
		r.clusterDepartment = department
	}
}

func (r *ReverseProxy) GetProxy(name string) *httputil.ReverseProxy {
	r.RLock()
	defer r.RUnlock()
	return r.routes[name]
}

func (r *ReverseProxy) GetNames() []string {
	r.RLock()
	defer r.RUnlock()
	var names []string
	for name, _ := range r.routes {
		names = append(names, name)
	}
	return names
}

func (r *ReverseProxy) GetClusters() []Clusters {
	departmentClusters := make(map[string][]string)
	r.RLock()
	for product, department := range r.clusterDepartment {
		if department == "" {
			department = "default"
		}
		departmentClusters[department] = append(departmentClusters[department], product)
	}
	r.RUnlock()
	var cluster []Clusters
	for d, cs := range departmentClusters {
		sort.Sort(sort.StringSlice(cs))
		cluster = append(cluster, Clusters{
			DepartmentName: d,
			ClusterList:    cs,
		})
	}
	sort.Sort(ClusterSorter(cluster))

	return cluster
}

type Clusters struct {
	DepartmentName string   `json:"departmentName"`
	ClusterList    []string `json:"clusterList"`
}

type ClusterSorter []Clusters

func (p ClusterSorter) Len() int           { return len(p) }
func (p ClusterSorter) Less(i, j int) bool { return p[i].DepartmentName < p[j].DepartmentName }
func (p ClusterSorter) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
