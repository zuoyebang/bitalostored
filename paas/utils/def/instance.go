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
	NODE_STATUS_ONLINE  = "online"
	NODE_STATUS_OFFLINE = "offline"
	NODE_STATUS_NEW     = "new"
)

const (
	CLUSTER_STATUS_ONLINE  = "online"
	CLUSTER_STATUS_OFFLINE = "offline"
)

const (
	GROUP_STATUS_ONLINE  = "online"
	GROUP_STATUS_OFFLINE = "offline"
)

const (
	START_MODEL_NORMAL   = "normal"
	START_MODEL_OBSERVER = "observer"
)

const (
	NODE_ROLE_OBSERVER = "observer"
	NODE_ROLE_WITNESS  = "witness"
	NODE_ROLE_SLAVE    = "slave"
	NODE_ROLE_MASTER   = "master"
	NODE_ROLE_SINGLE   = "single"
)

const (
	DashboardCloud  = "dh_cloud"
	DashboardMaster = "master"
	DashboardBackup = "backup"
)

const (
	NODE_IS_WITNESS  = 1
	NODE_NOT_WITNESS = 0
)
