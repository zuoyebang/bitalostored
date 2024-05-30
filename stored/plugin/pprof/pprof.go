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

package pprof

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

func Init() {
	if !config.GlobalConfig.Plugin.OpenPprof {
		return
	}

	server.AddPlugin(&server.Proc{Start: func(s *server.Server) {
		go func() {
			pprofAddr := config.GlobalConfig.Plugin.PprofAddr
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Errorf("pprof ListenAndServe err:%v", err)
			} else {
				log.Infof("pprof addr:%s", pprofAddr)
			}
		}()
	}})
}
