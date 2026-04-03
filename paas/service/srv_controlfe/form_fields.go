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

package srv_controlfe

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
)

type FormControlInput struct {
}

var _ servicer.Servicer = new(FormControlInput)

func (input *FormControlInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *FormControlInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var formControl FormControlOutput
	formControl.RegionInfos = regionInfos()
	formControl.ServiceInfos = serviceInfos()
	formControl.PackageInfos = packageInfos()
	formControl.FileInfos = fileInfos()
	formControl.ClusterInfos = clusterInfos()
	formControl.StoredInfos = storedInfos()
	formControl.MachineInfos = machineInfos()
	formControl.MachineStatInfos = machineStatInfos()
	formControl.MatrixInfos = matrixInfos()
	formControl.ProxyInfos = proxyInfos()
	formControl.RecentTaskInfos = recentTaskInfos()
	formControl.UnreleasedTaskInfos = unreleasedTaskInfos()
	formControl.CosFileList = cosFileList()
	formControl.MachineInfoList = machineInfoList()
	return formControl, nil
}

func serviceInfos() []ControlElement {
	return []ControlElement{
		{Text: "serviceId", Value: "serviceId", Sortable: true},
		{Text: "serviceName", Value: "serviceName", Sortable: true},
		{Text: "clusterPortRange", Value: "clusterPortRange"},
		{Text: "portRange", Value: "portRange"},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func packageInfos() []ControlElement {
	return []ControlElement{
		{Text: "serviceId", Value: "serviceId", Sortable: true},
		{Text: "packageId", Value: "packageId", Sortable: true},
		{Text: "packageTag", Value: "packageTag", Sortable: true},
		{Text: "arg", Value: "arg", Sortable: true},
		{Text: "env", Value: "env", Sortable: true},
		{Text: "fileCount", Value: "fileCount", Sortable: true},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
		//{Text: "Actions", Value: "actions", Sortable: false},
	}
}

func fileInfos() []ControlElement {
	return []ControlElement{
		{Text: "resourceId", Value: "resourceId", Sortable: true},
		{Text: "packageId", Value: "packageId", Sortable: true},
		{Text: "fileType", Value: "fileType"},
		{Text: "fileMode", Value: "fileMode"},
		{Text: "fileName", Value: "fileName", Sortable: true},
		{Text: "fileSize", Value: "fileSize", Sortable: true},
		{Text: "fileHash", Value: "fileHash"},
		{Text: "addressLink", Value: "addressLink"},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func regionInfos() []ControlElement {
	return []ControlElement{
		{Text: "regionId", Value: "regionId", Sortable: true},
		{Text: "regionName", Value: "regionName", Sortable: true},
		{Text: "matrix", Value: "matrix", Sortable: false},
		{Text: "proxy", Value: "proxy", Sortable: false},
		{Text: "machineSum", Value: "machineSum", Sortable: false},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func clusterInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "clusterName", Value: "clusterName", Sortable: true},
		//{Text: "regionName", Value: "regionName", Sortable: true},
		{Text: "department", Value: "department", Sortable: true},
		{Text: "deraftToken", Value: "deraftToken", Sortable: false},
		//{Text: "storedAuth", Value: "storedAuth", Sortable: false},
		{Text: "clusterStatus", Value: "clusterStatus", Sortable: true},
		//{Text: "configPackId", Value: "configPackId", Sortable: true},
		//{Text: "regionId", Value: "regionId", Sortable: true},
		//{Text: "storedId", Value: "storedId", Sortable: true},
		//{Text: "serviceId", Value: "serviceId", Sortable: false},
		{Text: "dashboardAddr", Value: "dashboardAddr", Sortable: true},
		{Text: "createTime", Value: "createTime", Sortable: true},
		// {Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func storedInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "clusterName", Value: "clusterName", Sortable: true},
		{Text: "storedId", Value: "storedId", Sortable: true},
		//{Text: "configPackId", Value: "configPackId", Sortable: true},
		{Text: "version", Value: "version", Sortable: true},
		//{Text: "nodeId", Value: "nodeId", Sortable: true},
		{Text: "nodeStatus", Value: "nodeStatus", Sortable: true},
		//{Text: "regionId", Value: "regionId", Sortable: true},
		//{Text: "regionName", Value: "regionName", Sortable: false},
		{Text: "department", Value: "department", Sortable: false},
		{Text: "ip", Value: "ip", Sortable: false},
		//{Text: "budget", Value: "budget", Sortable: false},
		{Text: "servicePort", Value: "servicePort", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: false},
	}
}

func machineStatInfos() []ControlElement {
	return []ControlElement{
		{Text: "machineId", Value: "machineId", Sortable: true},
		{Text: "machineStatus", Value: "machineStatus", Sortable: true},
		{Text: "ip", Value: "ip", Sortable: true},
		{Text: "idc", Value: "idc", Sortable: true},
		{Text: "agentStatus", Value: "agentStatus", Sortable: true},
		{Text: "clusterNodeSum", Value: "clusterNodeSum", Sortable: true},
		{Text: "totalNodeSum", Value: "totalNodeSum", Sortable: true},
		{Text: "balanceIndex", Value: "balanceIndex", Sortable: true},
	}
}

func machineInfos() []ControlElement {
	return []ControlElement{
		{Text: "machineId", Value: "machineId", Sortable: true},
		{Text: "ip", Value: "ip", Sortable: true},
		{Text: "idc", Value: "idc", Sortable: true},
		{Text: "weight", Value: "weight", Sortable: true},
		{Text: "regionId", Value: "regionId", Sortable: true},
		{Text: "machineStatus", Value: "machineStatus", Sortable: true},
		{Text: "agentStatus", Value: "agentStatus", Sortable: true},
		{Text: "agentVersion", Value: "agentVersion", Sortable: true},
		{Text: "isVirtual", Value: "isVirtual", Sortable: true},
		{Text: "agentStartTime", Value: "agentStartTime", Sortable: true},
		{Text: "instanceStat", Value: "instanceStat", Sortable: true},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func nodeInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "groupId", Value: "groupId", Sortable: true},
		{Text: "nodeId", Value: "nodeId", Sortable: true},
		{Text: "nodeStatus", Value: "nodeStatus", Sortable: true},
		{Text: "regionId", Value: "regionId", Sortable: true},
		{Text: "machineId", Value: "machineId", Sortable: true},
		{Text: "ip", Value: "ip", Sortable: false},
		{Text: "serviceId", Value: "serviceId", Sortable: true},
		{Text: "serviceName", Value: "serviceName", Sortable: false},
		{Text: "packageId", Value: "packageId", Sortable: true},
		{Text: "packageTag", Value: "packageTag", Sortable: false},
		{Text: "locked", Value: "locked", Sortable: false},
		{Text: "servicePort", Value: "servicePort", Sortable: true},
		{Text: "clusterPort", Value: "clusterPort", Sortable: true},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func matrixInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "clusterName", Value: "clusterName", Sortable: true},
		{Text: "groupId", Value: "groupId", Sortable: true},
		{Text: "nodeId", Value: "nodeId", Sortable: true},
		{Text: "nodeStatus", Value: "nodeStatus", Sortable: true},
		{Text: "regionId", Value: "regionId", Sortable: true},
		{Text: "machineId", Value: "machineId", Sortable: true},
		{Text: "ip", Value: "ip", Sortable: false},
		{Text: "idc", Value: "idc", Sortable: false},
		{Text: "version", Value: "version", Sortable: true},
		{Text: "serviceName", Value: "serviceName", Sortable: true},
		{Text: "locked", Value: "locked", Sortable: false},
		{Text: "witness", Value: "witness", Sortable: false},
		// {Text: "servicePort", Value: "servicePort", Sortable: true},
		{Text: "clusterPort", Value: "clusterPort", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func proxyInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "clusterName", Value: "clusterName", Sortable: true},
		{Text: "nodeId", Value: "nodeId", Sortable: true},
		{Text: "nodeStatus", Value: "nodeStatus", Sortable: true},
		{Text: "regionId", Value: "regionId", Sortable: true},
		{Text: "regionName", Value: "regionName", Sortable: false},
		{Text: "machineId", Value: "machineId", Sortable: true},
		{Text: "ip", Value: "ip", Sortable: false},
		{Text: "serviceId", Value: "serviceId", Sortable: true},
		{Text: "version", Value: "version", Sortable: false},
		{Text: "matrixProxy", Value: "matrixProxy", Sortable: false},
		{Text: "servicePort", Value: "servicePort", Sortable: true},
		{Text: "clusterPort", Value: "clusterPort", Sortable: true},
		{Text: "dashboardAddr", Value: "dashboardAddr", Sortable: false},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func recentTaskInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "groupId", Value: "groupId", Sortable: true},
		{Text: "nodeId", Value: "nodeId", Sortable: true},
		{Text: "taskId", Value: "taskId", Sortable: true},
		{Text: "taskType", Value: "taskType", Sortable: true},
		{Text: "taskStatus", Value: "taskStatus", Sortable: true},
		{Text: "server", Value: "server", Sortable: true},
		{Text: "service", Value: "service", Sortable: true},
		{Text: "extra", Value: "extra", Sortable: true},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func unreleasedTaskInfos() []ControlElement {
	return []ControlElement{
		{Text: "clusterId", Value: "clusterId", Sortable: true},
		{Text: "groupId", Value: "groupId", Sortable: true},
		{Text: "nodeId", Value: "nodeId", Sortable: true},
		{Text: "taskId", Value: "taskId", Sortable: true},
		{Text: "taskType", Value: "taskType", Sortable: true},
		{Text: "taskStatus", Value: "taskStatus", Sortable: true},
		{Text: "server", Value: "server", Sortable: true},
		{Text: "service", Value: "service", Sortable: true},
		{Text: "extra", Value: "extra"},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func cosFileList() []ControlElement {
	return []ControlElement{
		{Text: "id", Value: "id", Sortable: true},
		{Text: "name", Value: "name", Sortable: true},
		{Text: "fileType", Value: "fileType"},
		{Text: "fileMode", Value: "fileMode"},
		{Text: "version", Value: "version", Sortable: true},
		{Text: "hash", Value: "hash"},
		{Text: "clusterList", Value: "clusterList", Sortable: false},
		{Text: "createTime", Value: "createTime", Sortable: true},
		{Text: "updateTime", Value: "updateTime", Sortable: true},
	}
}

func machineInfoList() []ControlElement {
	return []ControlElement{
		{Text: "machineId", Value: "machineId", Sortable: true},
		{Text: "ip", Value: "ip", Sortable: true},
		{Text: "status", Value: "status", Sortable: true},
		{Text: "budget", Value: "budget", Sortable: true},
		{Text: "idc", Value: "idc", Sortable: true},
		{Text: "cpuTotal", Value: "cpuTotal", Sortable: true},
		{Text: "cpuSetMax", Value: "cpuSetMax", Sortable: true},
		{Text: "isVirtual", Value: "isVirtual", Sortable: true},
		{Text: "nodes", Value: "nodes", Sortable: false},
		{Text: "regions", Value: "regions", Sortable: false},
		{Text: "agentStatus", Value: "agentStatus", Sortable: true},
		{Text: "version", Value: "version", Sortable: true},
	}
}

//	func configList() []ControlElement {
//		return []ControlElement{
//			{Text: "id", Value: "id", Sortable: true},
//			{Text: "name", Value: "name", Sortable: true},
//			{Text: "fileType", Value: "fileType"},
//			{Text: "fileMode", Value: "fileMode"},
//			{Text: "version", Value: "version", Sortable: true},
//			{Text: "idc", Value: "idc", Sortable: true},
//			{Text: "content", Value: "content"},
//			{Text: "createTime", Value: "createTime", Sortable: true},
//			{Text: "updateTime", Value: "updateTime", Sortable: true},
//		}
//	}
type FormControlOutput struct {
	NodeInfos           []ControlElement `json:"nodeInfos"`
	MatrixInfos         []ControlElement `json:"matrixInfos"`
	ProxyInfos          []ControlElement `json:"proxyInfos"`
	FileInfos           []ControlElement `json:"fileInfos"`
	PackageInfos        []ControlElement `json:"packageInfos"`
	ServiceInfos        []ControlElement `json:"serviceInfos"`
	RegionInfos         []ControlElement `json:"regionInfos"`
	ClusterInfos        []ControlElement `json:"clusterInfos"`
	StoredInfos         []ControlElement `json:"storedInfos"`
	RecentTaskInfos     []ControlElement `json:"recentTaskInfos"`
	UnreleasedTaskInfos []ControlElement `json:"unreleasedTaskInfos"`
	MachineInfos        []ControlElement `json:"machineInfos"`
	MachineStatInfos    []ControlElement `json:"machineStatInfos"`
	CosFileList         []ControlElement `json:"cosFileList"`
	ConfigList          []ControlElement `json:"configList"`
	MachineInfoList     []ControlElement `json:"machineInfoList"`
}

type ControlElement struct {
	Text     string `json:"text"`
	Value    string `json:"value"`
	Sortable bool   `json:"sortable"`
}
