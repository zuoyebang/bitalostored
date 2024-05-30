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

package catch_panic

import (
	"runtime"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func Init() {
	if !config.GlobalConfig.Plugin.OpenPanic {
		return
	}

	server.AddPlugin(&server.Proc{Disconn: func(s *server.Server, c *server.Client, err interface{}) {
		if err != nil {
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			log.Errorf("client run panic err:%v stack:%s", err, unsafe2.String(buf[0:n]))
		}
	}})
}
