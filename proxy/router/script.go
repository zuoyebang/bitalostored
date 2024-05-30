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

import "github.com/zuoyebang/bitalostored/proxy/resp"

func (pc *ProxyClient) EvalCommon(command string, s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(command, s, args...)
}

func (pc *ProxyClient) ScriptExists(command string, s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(command, s, args...)
}

func (pc *ProxyClient) ScriptFlush(s *resp.Session) (interface{}, error) {
	_, err := pc.do(resp.SCRIPT, s, "flush")
	return nil, err
}

func (pc *ProxyClient) ScriptLen(s *resp.Session) (interface{}, error) {
	return pc.do(resp.SCRIPT, s, "len")
}

func (pc *ProxyClient) ScriptLoad(s *resp.Session, script string) (interface{}, error) {
	return pc.do(resp.SCRIPT, s, "load", script)
}
