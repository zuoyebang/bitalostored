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

import (
	"regexp"
	"strconv"
	"strings"
)

func GetMajorVersion(ver string) int {
	if ver == "" {
		return 0
	}
	regex, err := regexp.Compile(`^v(\d+)`)
	if err != nil {
		return 0
	}

	matches := regex.FindStringSubmatch(ver)
	if len(matches) > 0 {
		v, _ := strconv.Atoi(matches[1])
		return v
	}
	return 0
}

func CheckVersion(oldVersion, newVersion string) bool {
	if oldVersion == newVersion {
		return true
	}
	isOldVer, _ := regexp.MatchString("^(v)([0-9]+)(\\.([0-9]+)){2}$", oldVersion)
	isNewVer, _ := regexp.MatchString("^(v)([0-9]+)(\\.([0-9]+)){2}$", newVersion)
	if !isOldVer || !isNewVer {
		return true
	}
	oldParts := strings.Split(strings.TrimPrefix(oldVersion, "v"), ".")
	newParts := strings.Split(strings.TrimPrefix(newVersion, "v"), ".")

	oldMajor, _ := strconv.Atoi(oldParts[0])
	oldMinor, _ := strconv.Atoi(oldParts[1])
	newMajor, _ := strconv.Atoi(newParts[0])
	newMinor, _ := strconv.Atoi(newParts[1])
	if newMajor > 8 || (newMajor == 8 && newMinor >= 5) {
		if oldMajor < 8 || (oldMajor == 8 && oldMinor < 5) {
			return false
		}
	}

	if oldMajor > 8 || (oldMajor == 8 && oldMinor >= 5) {
		if newMajor < 8 || (newMajor == 8 && newMinor < 5) {
			return false
		}
	}
	return true
}

func CheckSingleCloudDomain(domain string) bool {
	sp := strings.Split(domain, ".")
	sp1 := strings.Split(sp[0], "-")
	cloudMaybe := sp1[len(sp1)-1]
	switch cloudMaybe {
	case "tx", "bd", "ali":
		return true
	}
	return false
}
