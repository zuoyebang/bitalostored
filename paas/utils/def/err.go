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
	PARAM_ERROR     = 20001
	TOKEN_ERROR     = 30001
	FORBIDDEN_ERROR = 30002
	IpsRedirect     = 30003
	UNKNOWN_ERROR   = 99999
)

var ErrMsg = map[int]string{
	PARAM_ERROR:     "param_error",
	UNKNOWN_ERROR:   "unknown_error",
	FORBIDDEN_ERROR: "forbidden_error",
}
