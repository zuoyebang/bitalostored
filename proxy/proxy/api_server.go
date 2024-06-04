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

package proxy

import (
	"errors"
	"net/http"
	"runtime"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/models"
	"github.com/zuoyebang/bitalostored/proxy/internal/rpc"
	"github.com/zuoyebang/bitalostored/proxy/internal/switcher"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"
)

type apiServer struct {
	proxy *Proxy
}

func newApiServer(p *Proxy) http.Handler {
	m := martini.New()
	m.Use(martini.Recovery())
	m.Use(render.Renderer())
	m.Use(func(w http.ResponseWriter, req *http.Request, c martini.Context) {
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
			log.Warnf("[%p] API call %s from %s [%s]", p, path, remoteAddr, headerAddr)
		}
		c.Next()
	})
	m.Use(gzip.All())
	m.Use(func(c martini.Context, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	})

	api := &apiServer{proxy: p}

	r := martini.NewRouter()
	r.Get("/", func(r render.Render) {
		r.Redirect("/proxy")
	})
	r.Any("/debug/**", func(w http.ResponseWriter, req *http.Request) {
		http.DefaultServeMux.ServeHTTP(w, req)
	})

	r.Group("/proxy", func(r martini.Router) {
		r.Get("", api.Overview)
		r.Get("/model", api.Model)
		r.Get("/stats", api.StatsNoXAuth)
		r.Get("/slots", api.SlotsNoXAuth)
	})
	r.Group("/api/proxy", func(r martini.Router) {
		r.Get("/model", api.Model)
		r.Get("/xping/:xauth", api.XPing)
		r.Get("/stats/:xauth", api.Stats)
		r.Get("/stats/:xauth/:flags", api.Stats)
		r.Get("/slots/:xauth", api.Slots)
		r.Get("/pconfig/:xauth", api.GetPconfigs)
		r.Put("/start/:xauth", api.Start)
		r.Put("/stats/reset/:xauth", api.ResetStats)
		r.Put("/forcegc/:xauth", api.ForceGC)
		r.Put("/shutdown/:xauth", api.Shutdown)
		r.Put("/readcrosscloud/:xauth/:flag", api.SetReadCrossCloudFlag)
		r.Put("/fillslots/:xauth", binding.Json([]*models.Slot{}), api.FillSlots)
		r.Put("/fillpconfigs/:xauth", binding.Json([]*models.Pconfig{}), api.FillPconfigs)
	})

	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)
	return m
}

func (s *apiServer) verifyXAuth(params martini.Params) error {
	if s.proxy.IsClosed() {
		return errn.ErrClosedProxy
	}
	xauth := params["xauth"]
	if xauth == "" {
		return errors.New("missing xauth, please check product name & auth")
	}
	if xauth != s.proxy.XAuth() {
		return errors.New("invalid xauth, please check product name & auth")
	}
	return nil
}

func (s *apiServer) Overview() (int, string) {
	return rpc.ApiResponseJson(GetOverview(s.proxy, StatsFull))
}

func (s *apiServer) Model() (int, string) {
	return rpc.ApiResponseJson(s.proxy.Model())
}

func (s *apiServer) StatsNoXAuth() (int, string) {
	return rpc.ApiResponseJson(GetStats(s.proxy, false))
}

func (s *apiServer) SlotsNoXAuth() (int, string) {
	return rpc.ApiResponseJson(ShortSlots())
}

func (s *apiServer) XPing(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) Stats(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(GetSimpleStats())
	}
}

func (s *apiServer) Slots(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return s.SlotsNoXAuth()
	}
}

func (s *apiServer) GetPconfigs(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(Pconfigs())
	}
}

func (s *apiServer) Start(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.proxy.Start(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ResetStats(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		dostats.ResetStats()
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ForceGC(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		runtime.GC()
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) Shutdown(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.proxy.Close(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SetReadCrossCloudFlag(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	flagStr := params["flag"]
	flag, err := strconv.Atoi(flagStr)
	if err != nil {
		return rpc.ApiResponseError(errors.New("invalid param.flag " + err.Error()))
	}
	if s.proxy.Config().ReadCrossCloud == config.CrossCloudOverwrite {
		switcher.ReadCrossCloud.Store(!(flag == 0))
	}

	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) FillSlots(slots []*models.Slot, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := FillSlots(slots); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) FillPconfigs(pconfigs []*models.Pconfig, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	log.Infof("FillPconfigs : %v", pconfigs)

	if err := FillPconfigs(pconfigs); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}
