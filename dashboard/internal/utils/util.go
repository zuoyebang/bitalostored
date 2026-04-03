// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

const (
	TX  = "txcloud"
	ALI = "ali"
	BD  = "baidu"
)

func FmtDuration(d time.Duration) string {
	if d > time.Second {
		return fmt.Sprintf("%d.%03ds", d/time.Second, d/time.Millisecond%1000)
	}
	if d > time.Millisecond {
		return fmt.Sprintf("%d.%03dms", d/time.Millisecond, d/time.Microsecond%1000)
	}
	if d > time.Microsecond {
		return fmt.Sprintf("%d.%03dus", d/time.Microsecond, d%1000)
	}
	return fmt.Sprintf("%dns", d)
}

func FmtSize(fileSize float64) string {
	switch {
	case fileSize >= PB:
		return fmt.Sprintf("%.2fPB", fileSize/PB)
	case fileSize >= TB:
		return fmt.Sprintf("%.2fTB", fileSize/TB)
	case fileSize >= GB:
		return fmt.Sprintf("%.2fGB", fileSize/GB)
	case fileSize >= MB:
		return fmt.Sprintf("%.2fMB", fileSize/MB)
	case fileSize >= KB:
		return fmt.Sprintf("%.2fKB", fileSize/KB)
	}
	return fmt.Sprintf("%.2fB", fileSize)
}

func MapIsEqual(a map[int]string, b map[int]string) bool {
	if len(a) != len(b) {
		return false
	} else {
		for i, _ := range a {
			if a[i] != b[i] {
				return false
			}
		}
	}
	return true
}

func RemoveRepeatedElement(arr []string) (newArr []string) {
	set := make(map[string]struct{}, len(arr))
	j := 0
	for _, v := range arr {
		_, ok := set[v]
		if ok {
			continue
		}
		set[v] = struct{}{}
		arr[j] = v
		j++
	}
	return arr[:j]
}

func CheckVersionGEV7(ver string) bool {
	return GetMajorVersion(ver) >= 7
}

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

func ServerGroupKey(addr string, gid int) string {
	return addr + "-" + strconv.Itoa(gid)
}

func ConvertInfoMap(text string) map[string]string {
	info := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" {
			info[key] = strings.TrimSpace(kv[1])
		}
	}
	return info
}

func RemoveAllElement(slice []string, value string) []string {
	newSlice := make([]string, 0, len(slice))
	for _, v := range slice {
		if v != value {
			newSlice = append(newSlice, v)
		}
	}
	return newSlice
}
