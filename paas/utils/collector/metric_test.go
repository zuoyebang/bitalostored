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

package collector

import (
	"fmt"
	"testing"
)

func TestMetricLine(t *testing.T) {
	line := `stored_machine_cdm_ops_fails{idc="ali",machine="10.33.36.10",name="ocr-search",port="8116",type="proxy"} 0`
	fmt.Println(formatLineInfo(line))
}

func TestGetAllMetrics(t *testing.T) {
	NewGrafanaCollector()
	fmt.Println(GetAllMachineMetrics())
}
