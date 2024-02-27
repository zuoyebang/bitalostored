// Copyright 2019 The Bitalostored author and other contributors.
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

package butils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func GetDirSize(dir string) int64 {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return 0
	}
	res := strings.Split(Command("du", "-sb", dir), "\t")
	if len(res) < 2 || len(res) > 2 {
		return 0
	}
	size, err := strconv.ParseInt(res[0], 10, 64)
	if err != nil {
		return 0
	}
	return size
}

func Command(key string, arg ...string) string {
	cmd := exec.Command(key, arg...)
	b, _ := cmd.CombinedOutput()
	return strings.TrimSpace(string(b))
}

func DirSize(path string) (float64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	sizeKB := float64(size) / 1024.0
	return sizeKB, err
}

func DirCreate(path string) error {
	dirExists, err := IsPathExists(path)
	if err != nil {
		return fmt.Errorf("error checking if directory exists '%s': %w", path, err)
	}
	if !dirExists {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			return fmt.Errorf("error creating directory '%s': %w", path, err)
		}
	}
	return nil
}

func IsPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func IsFileNotExist(name string) bool {
	if len(name) == 0 {
		return true
	}
	_, err := os.Stat(name)
	return err != nil && os.IsNotExist(err)
}

func IsFileExist(name string) bool {
	if len(name) == 0 {
		return false
	}
	_, err := os.Stat(name)
	return err == nil || !os.IsNotExist(err)
}
