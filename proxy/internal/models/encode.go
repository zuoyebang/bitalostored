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

package models

import (
	"github.com/zuoyebang/bitalostored/proxy/internal/log"

	jsoniter "github.com/json-iterator/go"
)

func jsonEncode(v interface{}) []byte {
	b, err := jsoniter.Marshal(v)
	if err != nil {
		log.Warnf("encode to json failed err:%v", err)
	}
	return b
}
