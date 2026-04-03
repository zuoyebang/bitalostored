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

type TaskStatus string

const (
	SUCCESS TaskStatus = "success"
	FAIL    TaskStatus = "fail"
	DOING   TaskStatus = "doing"
	NEW     TaskStatus = "new"
)

type TaskType string

const (
	TASK_TYPE_PREPARESTART = "prepareStart"
	TASK_TYPE_PREPAREADD   = "prepareAdd"
	TASK_TYPE_START        = "start"
	TASK_TYPE_UPGRADE      = "upgrade"
	TASK_TYPE_OPERATE      = "operate"
	TASK_TYPE_ADD          = "add"
	TypeObConfig           = "obConfig"

	TASK_TYPE_CGROUP = "cgroup"
	TASK_TYPE_LINK   = "link"   // create soft link
)

const (
	TASK_SUCCESS   = "success"
	TASK_FAIL      = "fail"
	TASK_UNRELEASE = "toRelease"
	TASK_CANCEL    = "cancel"
	TASK_NEW       = "new"
)

const (
	TASK_OPERATION_SUPERVISOR_START = "supervisor-start"
)
