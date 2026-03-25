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
	OPERATION_START               = "start"
	OPERATION_START_FORGROUND     = "start-foreground"
	OPERATION_STOP                = "stop"
	OPERATION_RESTART             = "restart"
	OPERATION_STOP_FORCED         = "stop-forced"
	OPERATION_SUPERVISOR_RESTART  = "supervisor-restart"
	OPERATION_SUPERVISOR_STOP     = "supervisor-stop"
	OPERATION_SUPERVISOR_START    = "supervisor-start"
	OPERATION_MATRIX_UPGRADE      = "upgrade"
	OPERATION_BITALOS_UPGRADE     = "upgrade"
	OPERATION_DO_NOTHING          = "donothing"
	OPERATION_CGROUP_RELEASE_CPUS = "release_cpus"
	OPERATION_CGROUP_APPLY        = "apply_cgroup"
	OPERATION_CGROUP_SHARE_CPUS   = "apply_cgroup_share_cpus"
)

var (
	FEOperationList        = []string{OPERATION_RESTART, OPERATION_START, OPERATION_STOP}
	DashboardOperationList = []string{OPERATION_RESTART, OPERATION_START, OPERATION_STOP}
	ProxyOperationList     = []string{OPERATION_RESTART, OPERATION_SUPERVISOR_STOP, OPERATION_SUPERVISOR_START}
	MatrixOperationList    = []string{OPERATION_MATRIX_UPGRADE, OPERATION_SUPERVISOR_START, OPERATION_SUPERVISOR_STOP}
	BitalosOperationList   = []string{OPERATION_BITALOS_UPGRADE, OPERATION_SUPERVISOR_START, OPERATION_SUPERVISOR_STOP}
)
