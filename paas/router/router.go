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

package router

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/controller"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"net/http"
)

func SetRouter(request *gin.Engine) *gin.Engine {
	request.StaticFS("/bitalospaas/static", http.Dir(config.GetConf().FE.Assets))

	//login
	request.POST("/bitalospaas/login", controller.Login)
	// service
	request.GET("/bitalospaas/service/list", controller.ServiceList)
	request.GET("/bitalospaas/service/operations", controller.ServiceOperations)

	// region
	request.GET("/bitalospaas/region/list", controller.RegionList)
	request.POST("/bitalospaas/region/create", controller.CreateRegion)
	request.POST("/bitalospaas/region/bindmachines", controller.BindMachines)
	request.POST("/bitalospaas/region/unbindmachines", controller.UnbindMachine)
	request.POST("/bitalospaas/region/remove", controller.RemoveRegion)
	request.POST("/bitalospaas/region/removemachines", controller.RemoveRegionMachines)

	// machine
	request.GET("/bitalospaas/machine/all", controller.MachineAll)
	request.POST("/bitalospaas/machine/offline", controller.MachineOffline)
	request.POST("/bitalospaas/machine/markoffline", controller.MachineMarkOffline)
	request.POST("/bitalospaas/machine/multiremove", controller.MachineMultiRemove)
	request.POST("/bitalospaas/machine/register", controller.MachineRegister)
	request.POST("/bitalospaas/machine/addmulti", controller.MachineAddMulti)
	request.POST("/bitalospaas/machine/getbyip", controller.MachineGetByIp)
	request.POST("/bitalospaas/machine/update", controller.MachineUpdate)
	request.POST("/bitalospaas/machine/remove", controller.MachineRemove)
	request.GET("/bitalospaas/machineinfo/list", controller.MachineInfoList)
	request.GET("/bitalospaas/machine/budgetlist", controller.MachineBudgetList)
	request.GET("/bitalospaas/machine/hostports", controller.HostPortInfos)
	request.GET("/bitalospaas/machine/checkport", controller.CheckPort)
	request.GET("/bitalospaas/machine/infos", controller.MachineInfos)
	request.POST("/bitalospaas/machine/clusterinfo", controller.MachineClusterInfo)
	request.GET("/bitalospaas/machine/recovery", controller.MachineRecovery)
	request.GET("/bitalospaas/machine/nodedeployinfo", controller.MachineNodeDeployInfo)
	request.POST("/bitalospaas/machine/replicate", controller.MachineReplicate)
	request.POST("/bitalospaas/machine/migrate", controller.MachineMigrate)
	request.POST("/bitalospaas/machine/removeproxy", controller.MachineRemoveProxy)

	request.GET("/bitalospaas/agent/manageinfo", controller.AgentManageInfo)
	request.POST("/bitalospaas/agent/upgrade", controller.UpgradeAgent)

	// cluster
	request.POST("/bitalospaas/clustercreate/storedmatrix", controller.CreateMatrixCluster)
	request.POST("/bitalospaas/clustercreate/storedproxy", controller.CreateProxyCluster)
	request.POST("/bitalospaas/clustercreate/storeddashboard", controller.CreateDashboardCluster)
	request.POST("/bitalospaas/clustercreate/storedfe", controller.CreateFECluster)
	request.GET("/bitalospaas/clusterinfo/storedmatrix", controller.MatrixInfo)
	request.GET("/bitalospaas/clusterinfo/storedproxy", controller.ProxyInfo)
	request.GET("/bitalospaas/cluster/list", controller.ClusterList)
	request.GET("/bitalospaas/cluster/storedlist", controller.StoredList)
	request.POST("/bitalospaas/cluster/alignproxy", controller.AlignProxyCluster)
	request.POST("/bitalospaas/cluster/updatemonitor", controller.UpdateMonitor)
	request.POST("/bitalospaas/cluster/remove", controller.RemoveCluster)
	request.GET("/bitalospaas/cluster/markoffline", controller.MarkOffline)
	request.GET("/bitalospaas/cluster/deleteoffline", controller.DeleteOffline)
	request.POST("/bitalospaas/cluster/expansion", controller.Expand)
	request.POST("/bitalospaas/cluster/binddepartment", controller.BindDepartment)
	request.POST("/bitalospaas/cluster/offline", controller.Offline)
	request.GET("/bitalospaas/cluster/departmentlist", controller.DepartmentList)
	request.GET("/bitalospaas/cluster/exportinfo", controller.ClusterIPList)
	request.GET("/bitalospaas/cluster/syncall", controller.ClusterSyncAll)
	request.POST("/bitalospaas/cluster/replacedashboard", controller.ReplaceDashboard)
	request.POST("/bitalospaas/cluster/deployoverview", controller.DeployOverview)
	request.POST("/bitalospaas/cluster/deployinfo", controller.ClusterDeployInfo)
	request.POST("/bitalospaas/cluster/serverdetail", controller.ClusterDeployServerDetail)
	request.GET("/bitalospaas/cluster/clusternames", controller.ClusterName)
	request.POST("/bitalospaas/cluster/createall", controller.ClusterCreateAll)

	// group
	request.POST("/bitalospaas/group/create", controller.CreateGroups)
	request.POST("/bitalospaas/group/offline", controller.OfflineGroup)
	request.GET("/bitalospaas/group/infos", controller.GroupInfos)
	request.GET("/bitalospaas/dashboard/addmatrixes", controller.AddMatrix)
	request.POST("/bitalospaas/dashboard/replica", controller.Replica)
	request.POST("/bitalospaas/group/copy", controller.CopyGroup)
	request.POST("/bitalospaas/group/markoffline", controller.MarkGroupOffline)

	// node
	request.POST("/bitalospaas/node/add", controller.CreateNode)
	request.POST("/bitalospaas/node/offline", controller.OfflineNode)
	request.POST("/bitalospaas/node/multiupgrade", controller.NodeMultiUpgrade)
	request.POST("/bitalospaas/node/nomalmultiupgrade", controller.NormalMultiUpgrade)
	request.POST("/bitalospaas/node/upgrade", controller.NodeUpgrade)
	request.POST("/bitalospaas/node/operate", controller.NodeOperate)
	request.POST("/bitalospaas/group/reraft", controller.GroupReraft)
	request.GET("/bitalospaas/group/nodelist", controller.NodeList)
	request.GET("/bitalospaas/node/config", controller.NodeConfig)
	request.POST("/bitalospaas/node/addclusterwitness", controller.CreateClusterWitness)
	request.POST("/bitalospaas/node/removeclusterwitness", controller.RemoveClusterWitness)
	request.GET("/bitalospaas/node/updateport", controller.UpdatePort)

	// task
	request.GET("/bitalospaas/task/list", controller.TaskList)
	request.GET("/bitalospaas/task/serverhistory", controller.HistoryTasks)
	request.GET("/bitalospaas/task/recent", controller.RecentTasks)
	request.GET("/bitalospaas/task/unreleased", controller.UnreleasedTasks)
	request.POST("/bitalospaas/task/create", controller.TaskCreate)
	request.POST("/bitalospaas/task/hostport", controller.TaskHostPort)
	request.POST("/bitalospaas/task/prepared", controller.TaskPrepared)
	request.POST("/bitalospaas/task/started", controller.TaskStarted)
	request.POST("/bitalospaas/task/upgraded", controller.TaskUpgraded)
	request.POST("/bitalospaas/task/status", controller.TaskStatus)

	request.GET("/bitalospaas/controlfe/constantlist", controller.ConstantList)
	request.GET("/bitalospaas/controlfe/formfields", controller.FormFieldOrder)

	// manual
	request.GET("/bitalospaas/manual/unlockgroup", controller.UnlockGroup)

	//file
	request.GET("/bitalospaas/file/list", controller.FileList)
	request.POST("/bitalospaas/file/build", controller.FileBuild)
	request.POST("/bitalospaas/file/remove", controller.FileRemove)
	request.GET("/bitalospaas/config/list", controller.ConfigList)
	request.GET("/bitalospaas/config/packlist", controller.ConfigPackList)
	request.POST("/bitalospaas/config/bind", controller.ConfigBind)
	request.POST("/bitalospaas/config/update", controller.ConfigUpdate)
	request.POST("/bitalospaas/config/remove", controller.ConfigRemove)
	request.POST("/bitalospaas/config/replace", controller.ReplaceConf)
	request.GET("/bitalospaas/file/download", controller.FileDownload)
	request.POST("/bitalospaas/file/addlocal", controller.FileAddLocal)

	//resource pool
	request.POST("/bitalospaas/resource/addrecord", controller.AddResourceRecord)
	request.POST("/bitalospaas/resource/list", controller.ResourceList)
	request.POST("/bitalospaas/resource/editvalue", controller.EditValue)
	request.POST("/bitalospaas/resource/controlcost", controller.ControlCost)
	request.POST("/bitalospaas/resource/apply", controller.Apply)
	request.POST("/bitalospaas/resource/cpusetpermession", controller.CpuSetPermession)

	request.GET("/bitalospaas/operation/list", controller.List)
	return request
}
