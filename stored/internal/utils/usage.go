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

package utils

import "time"

func CPUUsage(d time.Duration) (float64, *Usage, error) {
	var now = time.Now()
	b, err := GetUsage()
	if err != nil {
		return 0, nil, err
	}
	time.Sleep(d)
	e, err := GetUsage()
	if err != nil {
		return 0, nil, err
	}
	usage := e.CPUTotal() - b.CPUTotal()
	return float64(usage) / float64(time.Since(now)), e, nil
}
