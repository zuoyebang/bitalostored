// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis_op

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCheckNodeStatus(t *testing.T) {
	timeTemplate := "2020-09-21 17:33:05.487444943 +0800 CST m=+0.024766611"
	strTimes := strings.SplitN(timeTemplate, ".", 2)
	if len(strTimes) != 2 {
		fmt.Println("error", timeTemplate)
		return
	}
	ti, e := time.Parse("2006-01-02 15:04:05", strTimes[0])
	if e != nil {
		fmt.Println(e.Error())
	}
	fmt.Println(ti)
}
