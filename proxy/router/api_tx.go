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

package router

import (
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"
)

func (pc *ProxyClient) Watch(s *resp.Session, args [][]byte) error {
	keys := make([]interface{}, 0, len(args))
	for _, value := range resp.StringSlice(args) {
		keys = append(keys, value)
	}
	_, err := pc.doWithClients("WATCH", s, keys...)
	return err
}

func (pc *ProxyClient) Unwatch(s *resp.Session) error {
	_, err := pc.doWithClients("UNWATCH", s)
	return err
}

func (pc *ProxyClient) Multi(s *resp.Session) error {
	_, err := pc.doWithClients("MULTI", s)
	return err
}

func (pc *ProxyClient) Exec(s *resp.Session) (prepareOk bool, err error) {
	_, err = pc.doWithClients("PREPARE", s)
	if err != nil {
		log.Warnf("prepare (in exec) error and discard err:%s", err.Error())
		pc.doWithClients("DISCARD", s)
		return false, err
	}

	if s.Recorder.CmdNum == 0 {
		return true, nil
	}

	pc.doWithClients("EXEC", s)
	return true, nil
}

func (pc *ProxyClient) Discard(s *resp.Session) error {
	pc.doWithClients("DISCARD", s)
	return nil
}
