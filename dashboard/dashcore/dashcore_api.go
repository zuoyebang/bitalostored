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
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/rpc"
	"github.com/zuoyebang/bitalostored/dashboard/internal/uredis"
	"github.com/zuoyebang/bitalostored/dashboard/models"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"
	"github.com/martini-contrib/sessions"
)

type apiServer struct {
	dashCore *DashCore
}

func newApiServer(d *DashCore) http.Handler {
	m := martini.New()
	m.Use(martini.Recovery())
	m.Use(render.Renderer())
	store := sessions.NewCookieStore([]byte("secret123"))
	m.Use(sessions.Sessions("utoken", store))

	m.Use(func(session sessions.Session, w http.ResponseWriter, req *http.Request, c martini.Context) {
		path := req.URL.Path
		if req.Method != "GET" && strings.HasPrefix(path, "/api/") {
			var remoteAddr = req.RemoteAddr
			var headerAddr string
			for _, key := range []string{"X-Real-IP", "X-Forwarded-For"} {
				if val := req.Header.Get(key); val != "" {
					headerAddr = val
					break
				}
			}
			log.Warnf("[%p] API call %s from %s [%s]", path, remoteAddr, headerAddr)
		}
		c.Next()
	})
	m.Use(gzip.All())
	m.Use(func(session sessions.Session, c martini.Context, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	})

	api := &apiServer{dashCore: d}

	r := martini.NewRouter()

	r.Get("/", func(r render.Render) {
		r.Redirect("/topom")
	})

	r.Post("/login", binding.Form(models.Admin{}), api.Login)
	r.Get("/logout", api.LogOut)

	r.Group("/topom", func(r martini.Router) {
		r.Get("", api.Overview)
		r.Get("/model", api.Model)
		r.Get("/stats", api.StatsNoXAuth)
		r.Get("/slots", api.SlotsNoXAuth)
	})
	r.Group("/api/topom", func(r martini.Router) {
		r.Get("/model", api.Model)
		r.Get("/xping/:xauth", api.XPing)
		r.Get("/stats/:xauth", api.Stats)
		r.Get("/slots/:xauth", api.Slots)
		r.Get("/migratelist/:xauth", api.MigrateList)
		r.Put("/department/:xauth/:value", api.UpdateDepartment)

		r.Put("/reload/:xauth", api.Reload)
		r.Put("/shutdown/:xauth", api.Shutdown)
		r.Put("/loglevel/:xauth/:value", api.LogLevel)
		r.Group("/proxy", func(r martini.Router) {
			r.Put("/create/:xauth/:addr", api.CreateProxy)
			r.Put("/online/:xauth/:addr", api.OnlineProxy)
			r.Put("/reinit/:xauth/:token", api.ReinitProxy)
			r.Put("/remove/:xauth/:token/:force", api.RemoveProxy)
			r.Put("/readcrosscloud/:xauth/:flag", api.ReadCrossCloud)
		})

		r.Group("/group", func(r martini.Router) {
			r.Put("/create/:xauth/:gid", api.CreateGroup)
			r.Put("/remove/:xauth/:gid", api.RemoveGroup)
			r.Put("/resync/:xauth/:gid", api.ResyncGroup)
			r.Put("/logcompact/:xauth/:gid", api.LogCompactGroup)

			r.Put("/resync-all/:xauth", api.ResyncGroupAll)
			r.Put("/add/:xauth/:gid/:addr/:cloudtype/:server_role", api.GroupAddServer)
			r.Put("/del/:xauth/:gid/:addr/:nodeid", api.GroupDelServer)
			r.Put("/mount/:xauth/:gid/:addr/:raftaddr/:nodeid/:model", api.GroupMountOrOfflineRaftNode)
			r.Put("/replica-groups/:xauth/:gid/:addr/:value", api.EnableReplicaGroups)
			r.Put("/replica-groups-all/:xauth/:value", api.EnableReplicaGroupsAll)
			r.Put("/deraftcluster/:xauth/:cloudtype/:token", api.DeRaftAllGroup)
			r.Put("/deraft/:xauth/:gid/:addr/:token", api.DeRaft)
			r.Put("/changerole/:xauth/:gid/:addr/:server_role", api.ChangeRole)

			r.Get("/getclustermembership/:xauth/:gid/:addr", api.GetClusterMembership)
			r.Get("/getnodehostinfo/:xauth/:gid/:addr", api.GetNodeHostInfo)

			r.Put("/promote/:xauth/:gid/:addr", api.GroupPromoteServer)

			r.Group("/action", func(r martini.Router) {
				r.Put("/create/:xauth/:addr", api.SyncCreateAction)
				r.Put("/remove/:xauth/:addr", api.SyncRemoveAction)
			})
			r.Get("/info/:addr", api.InfoServer)
			r.Get("/debuginfo/:addr", api.DebugInfoServer)
			r.Put("/compact/:xauth/:addr/:dbtype", api.Compact)
		})
		r.Group("/slots", func(r martini.Router) {
			r.Group("/action", func(r martini.Router) {
				r.Put("/create/init/:xauth", api.SlotCreateActionInit)
				r.Put("/create/:xauth/:sid/:gid", api.SlotCreateAction)
				r.Put("/create-some/:xauth/:src/:dst/:num", api.SlotCreateActionSome)
				r.Put("/create-range/:xauth/:beg/:end/:gid/:not_migrate", api.SlotCreateActionRange)
				r.Put("/remove/:xauth/:sid", api.SlotRemoveAction)
				r.Put("/disabled/:xauth/:value", api.SetSlotActionDisabled)
			})
			r.Put("/assign/:xauth", binding.Json([]*models.SlotMapping{}), api.SlotsAssignGroup)
			r.Put("/assign/:xauth/offline", binding.Json([]*models.SlotMapping{}), api.SlotsAssignOffline)
		})
		r.Group("/tools", func(r martini.Router) {
			r.Get("/whichgroupkey/:key", api.FindKeyGroup)
		})
		r.Group("/pconfig", func(r martini.Router) {
			r.Put("/update/:xauth", binding.Json(models.Pconfig{}), api.UpdatePconfig)
			r.Put("/resync-all/:xauth", api.ResyncAllPconfig)
			r.Put("/resync/:name/:xauth", api.ResyncOnePconfig)
			r.Get("/list/:xauth", api.ListPconfig)
			r.Get("/detail/:name", api.DetailPconfig)
		})
		r.Group("/admin", func(r martini.Router) {
			r.Get("/list", api.ListAdmin)
			r.Put("/add", binding.Json(models.Admin{}), api.AddAdmin)
			r.Put("/update", binding.Json(models.Admin{}), api.UpdateAdmin)
			r.Put("/del/:username", api.DelAdmin)
		})
	})

	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)
	return m
}

func (s *apiServer) verifyXAuth(params martini.Params) error {
	if s.dashCore.IsClosed() {
		return ErrClosedDashCore
	}
	xauth := params["xauth"]
	if xauth == "" {
		return errors.New("missing xauth, please check product name & auth")
	}
	if xauth != s.dashCore.XAuth() {
		return errors.New("invalid xauth, please check product name & auth")
	}
	return nil
}

func (s *apiServer) verifyLogin(session sessions.Session, req *http.Request) error {
	if s.dashCore.IsClosed() {
		return ErrClosedDashCore
	}
	if admin, err := s.dashCore.GetLoginAdmin(session); admin != nil && err == nil {
		if req.Method == "PUT" {
			path := req.URL.Path
			if strings.Contains(path, "/admin/") {
				if admin.CheckAddRolePower() {
					return nil
				}
				return errors.New("no power to manage people")
			} else if !admin.CheckOPRolePower() {
				return errors.New("no power to operation")
			}
		}
		return nil
	} else {
		return err
	}
}

func (s *apiServer) Overview() (int, string) {
	o, err := s.dashCore.Overview()
	if err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(o)
	}
}

func (s *apiServer) Model() (int, string) {
	return rpc.ApiResponseJson(s.dashCore.Model())
}

func (s *apiServer) StatsNoXAuth() (int, string) {
	if stats, err := s.dashCore.Stats(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(stats)
	}
}

func (s *apiServer) SlotsNoXAuth() (int, string) {
	if slots, err := s.dashCore.Slots(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(slots)
	}
}

func (s *apiServer) XPing(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) MigrateList(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		if migrateList, err := s.dashCore.GetMigrateList(); err != nil {
			return rpc.ApiResponseError(err)
		} else {
			return rpc.ApiResponseJson(migrateList)
		}
	}
}

func (s *apiServer) Stats(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return s.StatsNoXAuth()
	}
}

func (s *apiServer) UpdateDepartment(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	v := params["value"]
	if v == "" {
		return rpc.ApiResponseError(errors.New("missing value"))
	}

	if err := s.dashCore.UpdateDepartment(v); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		// 更新完刷新一下
		return s.Reload(session, req, params)
	}
}

func (s *apiServer) Slots(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return s.SlotsNoXAuth()
	}
}

func (s *apiServer) Reload(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.Reload(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) parseAddr(params martini.Params) (string, error) {
	addr := params["addr"]
	if addr == "" {
		return "", errors.New("missing addr")
	}
	return addr, nil
}

func (s *apiServer) parseRaftToken(params martini.Params) (string, error) {
	token := params["token"]
	if token == "" {
		return "", errors.New("missing raft token")
	}
	return token, nil
}

func (s *apiServer) parseRaftAddr(params martini.Params) (string, error) {
	addr := params["raftaddr"]
	if addr == "" {
		return "", errors.New("missing addr")
	}
	return addr, nil
}

func (s *apiServer) parsePconfigName(params martini.Params) (string, error) {
	name := params["name"]
	if name == "" {
		return "", errors.New("missing pconfig name")
	}
	return name, nil
}

func (s *apiServer) parseAdminName(params martini.Params) (string, error) {
	username := params["username"]
	if username == "" {
		return "", errors.New("missing admin username")
	}
	return username, nil
}

func (s *apiServer) parseCloudType(params martini.Params) (string, error) {
	cloudtype := params["cloudtype"]
	if cloudtype == "" {
		return "", errors.New("missing addr")
	}
	return cloudtype, nil
}

func (s *apiServer) parseToken(params martini.Params) (string, error) {
	token := params["token"]
	if token == "" {
		return "", errors.New("missing token")
	}
	return token, nil
}

func (s *apiServer) parseInteger(params martini.Params, entry string) (int, error) {
	text := params[entry]
	if text == "undefined" {
		return 0, nil
	}
	if text == "" {
		return 0, fmt.Errorf("missing %s", entry)
	}
	v, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", entry)
	}
	return v, nil
}

func (s *apiServer) CreateProxy(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.CreateProxy(addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) OnlineProxy(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.OnlineProxy(addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ReinitProxy(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.ReinitProxy(token); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ReadCrossCloud(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	flag := params["flag"]
	if flag == "" {
		return rpc.ApiResponseError(errors.Errorf("param error"))
	}
	s.dashCore.model.ReadCrossCloud = (flag != "0")
	if err := s.dashCore.ReadCrossCloud(flag); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) RemoveProxy(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	force, err := s.parseInteger(params, "force")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.RemoveProxy(token, force != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) CreateGroup(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.CreateGroup(gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) RemoveGroup(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.RemoveGroup(gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ResyncGroup(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.ResyncGroup(gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) LogCompactGroup(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.LogCompactGroup(gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ResyncGroupAll(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.ResyncGroupAll(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupAddServer(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	ct := params["cloudtype"]
	if len(ct) <= 0 {
		err = errors.New("missing xauth, please check product name & auth")
		return rpc.ApiResponseError(err)
	}
	serveRole := params["server_role"]
	if len(serveRole) <= 0 {
		err = errors.New("missing server_role")
		return rpc.ApiResponseError(err)
	}

	if !models.CheckInServerRole(serveRole) {
		err = errors.New("not in allow server_role")
		return rpc.ApiResponseError(err)
	}

	c, err := uredis.NewClient(addr, s.dashCore.Config().ProductAuth, time.Second)
	if err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return rpc.ApiResponseError(err)
	}
	defer c.Close()
	if err := s.dashCore.GroupAddServer(gid, serveRole, ct, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupDelServer(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	nodeId, err := s.parseInteger(params, "nodeid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.GroupDelServer(gid, addr, nodeId); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupMountOrOfflineRaftNode(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	raftaddr, err := s.parseRaftAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	nodeId, err := s.parseInteger(params, "nodeid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	model, err := s.parseInteger(params, "model")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.GroupMountOrOfflineNode(model, gid, addr, raftaddr, nodeId); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupPromoteServer(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.GroupPromoteServer(gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GetClusterMembership(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if data, err := s.dashCore.GetClusterMembership(gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(data)
	}
}

func (s *apiServer) GetNodeHostInfo(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if data, err := s.dashCore.GetNodeHostInfo(gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(data)
	}
}

func (s *apiServer) FindKeyGroup(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	key, ok := params["key"]
	if !ok || key == "" {
		return rpc.ApiResponseError(errors.New("missing param key"))
	}
	slotId := hash.Fnv32(unsafe2.ByteSlice(key)) % 1024
	return rpc.ApiResponseJson(fmt.Sprintf("slotId:%d(hash method fnv)", slotId))
}

func (s *apiServer) EnableReplicaGroups(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	n, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.EnableReplicaGroups(gid, addr, n != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) EnableReplicaGroupsAll(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	n, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.EnableReplicaGroupsAll(n != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) DeRaftAllGroup(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	cloudType, err := s.parseCloudType(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseRaftToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.DeraftAllGroup(token, cloudType); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) DeRaft(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseRaftToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.DeRaftGroup(gid, addr, token); err != nil {
		return rpc.ApiResponseError(err)
	}
	// 摘分片下其他节点的流量
	if err := s.dashCore.InverseReplicaGroupsAll(gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) ReRaft(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseRaftToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	port, err := s.parseInteger(params, "port")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if port < 10000 || port > 30000 {
		return rpc.ApiResponseError(errors.New("port range [10000,30000]"))
	}
	if err := s.dashCore.ReRaftGroup(gid, addr, token, port); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.InverseReplicaGroupsAll(gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) ChangeRole(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	serveRole := params["server_role"]
	if len(serveRole) <= 0 {
		err = errors.New("missing server_role")
		return rpc.ApiResponseError(err)
	}
	if !models.CheckInServerRole(serveRole) {
		err = errors.New("not in allow server_role")
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.ChangeServerRole(gid, serveRole, addr); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) Compact(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	dbType := params["dbtype"]
	if dbType == "" {
		return rpc.ApiResponseError(errors.New("missing dbtype"))
	}
	if err := s.dashCore.Compact(addr, dbType); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) InfoServer(params martini.Params) (int, string) {
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	c, err := uredis.NewClient(addr, s.dashCore.Config().ProductAuth, time.Second)
	if err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return rpc.ApiResponseError(err)
	}
	defer c.Close()
	if info, err := c.InfoFull(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(info)
	}
}

func (s *apiServer) DebugInfoServer(params martini.Params) (int, string) {
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	c, err := uredis.NewClient(addr, s.dashCore.Config().ProductAuth, time.Second)
	if err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return rpc.ApiResponseError(err)
	}
	defer c.Close()
	if info, err := c.DebugInfoFull(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(info)
	}
}

func (s *apiServer) SyncCreateAction(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SyncCreateAction(addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SyncRemoveAction(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SyncRemoveAction(addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateAction(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	sid, err := s.parseInteger(params, "sid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SlotCreateAction(sid, gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateActionSome(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	groupFrom, err := s.parseInteger(params, "src")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	groupTo, err := s.parseInteger(params, "dst")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	numSlots, err := s.parseInteger(params, "num")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SlotCreateActionSome(groupFrom, groupTo, numSlots); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateActionRange(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	beg, err := s.parseInteger(params, "beg")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	end, err := s.parseInteger(params, "end")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	not_migrate, err := s.parseInteger(params, "not_migrate")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SlotCreateActionRange(beg, end, gid, true, not_migrate == 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateActionInit(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SlotCreateActionInit(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotRemoveAction(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	sid, err := s.parseInteger(params, "sid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SlotRemoveAction(sid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) LogLevel(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	v := params["value"]
	if v == "" {
		return rpc.ApiResponseError(errors.New("missing loglevel"))
	}
	if !log.SetLevelString(v) {
		return rpc.ApiResponseError(errors.New("invalid loglevel"))
	} else {
		log.Warnf("set loglevel to %s", v)
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) Shutdown(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.Close(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SetSlotActionDisabled(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	value, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	} else {
		s.dashCore.SetSlotActionDisabled(value != 0)
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotsAssignGroup(session sessions.Session, req *http.Request, slots []*models.SlotMapping, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	log.Info("SlotsAssignGroup:", slots)
	if err := s.dashCore.SlotsAssignGroup(slots); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) SlotsAssignOffline(session sessions.Session, req *http.Request, slots []*models.SlotMapping, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.SlotsAssignOffline(slots); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) ListAdmin(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if data, err := s.dashCore.GetAdminList(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		res := make([]*models.Admin, 0, len(data))
		for _, admin := range data {
			if admin.Role == models.SUPERADMIN {
				continue
			}
			res = append(res, admin.Snapshot())
		}
		return rpc.ApiResponseJson(res)
	}
}

func (s *apiServer) AddAdmin(session sessions.Session, req *http.Request, admin models.Admin, params martini.Params) (int, string) {
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	log.Info("AddAdmin:", admin)
	if len(admin.Username) <= 0 {
		return rpc.ApiResponseError(errors.Errorf("params err, admin : %v", admin))
	}

	if err := s.dashCore.CreateAdmin(&admin); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) UpdateAdmin(session sessions.Session, req *http.Request, admin models.Admin, params martini.Params) (int, string) {
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	log.Info("UpdateAdmin:", admin)

	if err := s.dashCore.UpdateAdmin(&admin); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) DelAdmin(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	username, err := s.parseAdminName(params)

	if err != nil {
		return rpc.ApiResponseError(err)
	}

	if err := s.dashCore.RemoveAdmin(username); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) AddPconfig(session sessions.Session, req *http.Request, pconfig models.Pconfig, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	log.Info("pconfig:", pconfig)
	if len(pconfig.Name) <= 0 {
		return rpc.ApiResponseError(errors.Errorf("params err, pconfig : %v", pconfig))
	}

	if err := s.dashCore.CreatePConfig(&pconfig); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) UpdatePconfig(session sessions.Session, req *http.Request, pconfig models.Pconfig, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	log.Info("pconfig:", pconfig)
	if err := s.dashCore.UpdatePConfig(&pconfig); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) DelPconfig(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	name, err := s.parsePconfigName(params)

	if err != nil {
		return rpc.ApiResponseError(err)
	}

	if err := s.dashCore.RemovePConfig(name); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) ResyncAllPconfig(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.dashCore.ResyncAllPconfig(); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) ResyncOnePconfig(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyLogin(session, req); err != nil {
		return rpc.ApiResponseError(err)
	}
	name, err := s.parsePconfigName(params)

	if err != nil {
		return rpc.ApiResponseError(err)
	}

	if err := s.dashCore.ResyncOnePconfig(name); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) ListPconfig(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}

	if data, err := s.dashCore.GetPConfigList(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		res := make([]*models.Pconfig, 0, len(data))
		for _, val := range data {
			res = append(res, val)
		}
		return rpc.ApiResponseJson(res)
	}
}

func (s *apiServer) DetailPconfig(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	name, err := s.parsePconfigName(params)

	if err != nil {
		return rpc.ApiResponseError(err)
	}

	if data, err := s.dashCore.GetPConfig(name); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(data)
	}
}

func (s *apiServer) Login(session sessions.Session, req *http.Request, admin models.Admin, params martini.Params) (int, string) {
	if admin, err := s.dashCore.AdminLogin(session, &admin); admin != nil && err == nil {
		return rpc.ApiResponseJson(admin)
	} else {
		return rpc.ApiResponseError(err)
	}
}

func (s *apiServer) LogOut(session sessions.Session, req *http.Request, params martini.Params) (int, string) {
	session.Delete(AdminKey)
	session.Options(sessions.Options{
		MaxAge: -1,
	})
	return rpc.ApiResponseJson("OK")
}
