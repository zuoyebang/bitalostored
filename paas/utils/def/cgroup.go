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

const LOCK_SYNC = "lock_cgroup_sync"
const LOCK_APPLY = "lock_cgroup_apply"

const EXCLUSIVE_CPU = 1
const SHARE_CPU = 2
const NOT_SET_CPU = 0

const DEFAULT_CGROUP_CPU int64 = 2

const (
	CGROUP_TASK_CPU       = "cpu"
	CGROUP_TASK_CPU_SET   = "cpu_set"
	CGROUP_TASK_CPU_SHARE = "share_cpu_set"
)

const (
	CGROUP_NAME_CPU = "cpu"
)
