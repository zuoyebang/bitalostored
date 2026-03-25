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

package def

const (
	SERVICE_MATRIX           = "stored-matrix"
	SERVICE_BITALOS          = "stored-bitalos"
	SERVICE_STORED_PROXY     = "stored-proxy"
	SERVICE_STORED_DASHBOARD = "stored-dashboard"
	SERVICE_STORED_FE        = "stored-fe"
	SERVICE_STORED_AGENT     = "stored-agent"
	SERVICE_STORED_PAAS      = "stored-paas"
	SERVICE_STORED_MONITOR   = "stored-monitor"
	SERVICE_STORED_TEST      = "stored-test"
	SERVICE_STORED_FE_ZIP    = "stored-fe-zip"
)

const (
	SERVICE_ID_MATRIX    = 1
	SERVICE_ID_PROXY     = 2
	SERVICE_ID_DASHBOARD = 3
	SERVICE_ID_FE        = 4
	SERVICE_ID_AGENT     = 5
	SERVICE_ID_BITALOS   = 6
)

func IsServer(serviceId uint) bool {
	switch serviceId {
	case SERVICE_ID_BITALOS, SERVICE_ID_MATRIX:
		return true
	default:
		return false
	}
}

func GetServiceNameFromServiceId(serviceId int) string {
	switch serviceId {
	case SERVICE_ID_MATRIX, SERVICE_ID_BITALOS:
		return SERVICE_BITALOS
	case SERVICE_ID_PROXY:
		return SERVICE_STORED_PROXY
	case SERVICE_ID_DASHBOARD:
		return SERVICE_STORED_DASHBOARD
	case SERVICE_ID_FE:
		return SERVICE_STORED_FE
	}
	return ""
}

type serviceBin struct {
	ServiceName string
	BinFileName string
}

var binFileNameMap = []serviceBin{
	{SERVICE_MATRIX, "bin/bitalostored"},
	{SERVICE_BITALOS, "bin/bitalostored"},
	{SERVICE_STORED_PROXY, "bin/bitalosproxy"},
	{SERVICE_STORED_DASHBOARD, "bin/bitalosdashboard"},
	{SERVICE_STORED_FE, "bin/bitalosfe"},
	{SERVICE_STORED_FE_ZIP, "bitalosfe.tar.zz"},
	{SERVICE_STORED_AGENT, "bin/bitalosagent"},
}

func GetBinNameByServiceName(serviceName string) string {
	for _, s := range binFileNameMap {
		if s.ServiceName == serviceName {
			return s.BinFileName
		}
	}
	return ""
}
