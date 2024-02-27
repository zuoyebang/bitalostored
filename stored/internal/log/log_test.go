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

package log

import (
	"os"
	"path"
	"testing"
)

func TestGlobalLog(t *testing.T) {
	dir := "./tmplog/"
	os.MkdirAll(path.Dir(dir), 0777)
	defer os.RemoveAll(dir)
	NewLogger(&Options{
		IsDebug:      false,
		RotationTime: DailyRotate,
		LogPath:      dir + "server",
	})

	Info("test Info ", "success")
	Warn("test Warn ", "success")
	Error("test Error ", "success")
	Debug("test Debug ", "success")
	Infof("test Infof %s", "success")
	Warnf("test Warnf %s", "success")
	Errorf("test Errorf %s", "success")
	Debugf("test Debugf %s", "success")
	Fatalf("test Fatalf %s", "success")
	SlowLog("127.0.0.1", 10, 4, [][]byte{[]byte("slow")}, nil)
}
