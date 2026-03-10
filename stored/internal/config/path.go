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

package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	SnapshotDirName  = "snapshot"
	DataDbDirName    = "bitalos"
	RaftMetaFileName = "BSMANIFEST"
)

func GetBitalosDbPath() string {
	return GlobalConfig.Server.DBPath
}

func GetBitalosDbDataPath() string {
	return filepath.Join(GetBitalosDbPath(), DataDbDirName)
}

func GetBitalosLogPath() string {
	return filepath.Join(GetBitalosDbPath(), "log", "bitalos")
}

func GetBitalosSnapshotPath() string {
	return filepath.Join(GetBitalosDbPath(), SnapshotDirName)
}

func GetSuffixSnapshotFileName(snapshotFilePath string) (string, error) {
	if strings.Contains(snapshotFilePath, SnapshotDirName) {
		divideArr := strings.Split(snapshotFilePath, SnapshotDirName)
		if len(divideArr) == 2 {
			suffixFileName := strings.Trim(divideArr[1], "/")
			if len(suffixFileName) > 0 {
				return suffixFileName, nil
			}
		}
	}
	return "", errors.New("snapshot filepath err")
}

func GetBitalosRaftDbsyncPath() string {
	return filepath.Join(GlobalConfig.Server.DBPath, "raft-dbsync")
}

func GetBitalosRaftWalPath() string {
	return filepath.Join(GlobalConfig.Server.DBPath, "raft-wallog")
}

func GetBitalosRaftNodeHostPath() string {
	return filepath.Join(GlobalConfig.Server.DBPath, "raft-nodehost")
}

func GetBitalosDataDbPath() string {
	return filepath.Join(GlobalConfig.Server.DBPath, DataDbDirName, "bitvector")
}

func GetRaftConfigPath() string {
	return filepath.Join(GlobalConfig.Server.DBPath, "raft-nodeconfig")
}

func GetRaftMetaPath() string {
	return filepath.Join(GlobalConfig.Server.DBPath, "raft-meta")
}

func GetRaftNodeMetaFile(clusterId uint64) string {
	return fmt.Sprintf("raft-%d.%s", clusterId, "meta")
}

func GetRaftMetaFile() string {
	return "raft.meta"
}
