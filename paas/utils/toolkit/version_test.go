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

package toolkit

import "testing"

func TestCheckVersion(t *testing.T) {
	oldVersion := "v8.5.0"
	newVersion := "v8.4.1"
	if CheckVersion(oldVersion, newVersion) {
		t.Errorf("old:%s new:%s", oldVersion, oldVersion)
	}

	oldVersion = "v8.4.4"
	newVersion = "v8.4.5"
	if !CheckVersion(oldVersion, newVersion) {
		t.Errorf("old:%s new:%s", oldVersion, oldVersion)
	}
}

func TestCheckSingle(t *testing.T) {
	host := "bitalos.cc"
	single := CheckSingleCloudDomain(host)
	t.Log(single)
}
