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

//go:build !linux

package utils

import (
	"syscall"
	"time"
)

// #include <unistd.h>
import "C"

type Usage struct {
	Utime  time.Duration `json:"utime"`
	Stime  time.Duration `json:"stime"`
	MaxRss int64         `json:"max_rss"`
	Ixrss  int64         `json:"ix_rss"`
	Idrss  int64         `json:"id_rss"`
	Isrss  int64         `json:"is_rss"`
}

func (u *Usage) MemTotal() int64 {
	return u.Ixrss + u.Idrss + u.Isrss
}

func (u *Usage) MemShr() int64 {
	return u.Ixrss
}

func (u *Usage) CPUTotal() time.Duration {
	return u.Utime + u.Stime
}

func GetUsage() (*Usage, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return nil, err
	}
	u := &Usage{}
	u.Utime = time.Duration(usage.Utime.Nano())
	u.Stime = time.Duration(usage.Stime.Nano())

	unit := 1024 * int64(C.sysconf(C._SC_CLK_TCK))

	u.MaxRss = usage.Maxrss
	u.Ixrss = unit * usage.Ixrss
	u.Idrss = unit * usage.Idrss
	u.Isrss = unit * usage.Isrss
	return u, nil
}
