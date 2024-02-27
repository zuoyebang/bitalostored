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

package utils

import (
	"strconv"

	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
)

func Argument(d map[string]interface{}, name string) (string, bool) {
	if d[name] != nil {
		if s, ok := d[name].(string); ok {
			if s != "" {
				return s, true
			}
			log.Panicf("option %s requires an argument", name)
		} else {
			log.Panicf("option %s isn't a valid string", name)
		}
	}
	return "", false
}

func ArgumentMust(d map[string]interface{}, name string) string {
	s, ok := Argument(d, name)
	if ok {
		return s
	}
	log.Panicf("option %s is required", name)
	return ""
}

func ArgumentInteger(d map[string]interface{}, name string) (int, bool) {
	if s, ok := Argument(d, name); ok {
		n, err := strconv.Atoi(s)
		if err != nil {
			log.PanicErrorf(err, "option %s isn't a valid integer", name)
		}
		return n, true
	}
	return 0, false
}
