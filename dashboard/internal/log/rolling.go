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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
)

type rollingFile struct {
	mu sync.Mutex

	closed bool

	file     *os.File
	basePath string
	filePath string
	fileFrag string

	rolling RollingFormat
}

var ErrClosedRollingFile = errors.New("rolling file is closed")

type RollingFormat string

const (
	MonthlyRolling  RollingFormat = "200601"
	DailyRolling    RollingFormat = "20060102"
	HourlyRolling   RollingFormat = "2006010215"
	MinutelyRolling RollingFormat = "200601021504"
	SecondlyRolling RollingFormat = "20060102150405"
	NoRolling       RollingFormat = ""
)

var rollingMap = map[string]RollingFormat{
	"MonthlyRolling":  MonthlyRolling,
	"DailyRolling":    DailyRolling,
	"HourlyRolling":   HourlyRolling,
	"MinutelyRolling": MinutelyRolling,
	"SecondlyRolling": SecondlyRolling,
	"NoRolling":       NoRolling,
}

func (r *rollingFile) roll() error {
	if r.rolling == NoRolling {
		if r.file != nil {
			return nil
		}
		r.filePath = r.basePath

		if dir, _ := filepath.Split(r.basePath); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0777); err != nil {
				return errors.Trace(err)
			}
		}

		f, err := os.OpenFile(r.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return errors.Trace(err)
		} else {
			r.file = f
			return nil
		}
	}

	suffix := time.Now().Format(string(r.rolling))
	if r.file != nil {
		if suffix == r.fileFrag {
			return nil
		}
		r.file.Close()
		r.file = nil
	}
	r.fileFrag = suffix
	r.filePath = fmt.Sprintf("%s.%s", r.basePath, r.fileFrag)

	if dir, _ := filepath.Split(r.basePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return errors.Trace(err)
		}
	}

	f, err := os.OpenFile(r.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return errors.Trace(err)
	} else {
		r.file = f
		return nil
	}
}

func (r *rollingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	if f := r.file; f != nil {
		r.file = nil
		return errors.Trace(f.Close())
	}
	return nil
}

func (r *rollingFile) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, errors.Trace(ErrClosedRollingFile)
	}

	if err := r.roll(); err != nil {
		return 0, err
	}

	n, err := r.file.Write(b)
	if err != nil {
		return n, errors.Trace(err)
	} else {
		return n, nil
	}
}

func NewRollingFile(basePath string, rolling RollingFormat) (io.WriteCloser, error) {
	if _, file := filepath.Split(basePath); file == "" {
		return nil, errors.Errorf("invalid base-path = %s, file name is required", basePath)
	}
	return &rollingFile{basePath: basePath, rolling: rolling}, nil
}
