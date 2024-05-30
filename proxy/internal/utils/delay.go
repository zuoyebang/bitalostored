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
	"time"

	"github.com/zuoyebang/bitalostored/butils/math2"
)

type DelayExp2 struct {
	Min, Max int
	Value    int
	Unit     time.Duration
}

func (d *DelayExp2) Reset() {
	d.Value = 0
}

func (d *DelayExp2) NextValue() int {
	d.Value = math2.MinMaxInt(d.Value*2, d.Min, d.Max)
	return d.Value
}

func (d *DelayExp2) After() <-chan time.Time {
	total := d.NextValue()
	return time.After(d.Unit * time.Duration(total))
}

func (d *DelayExp2) Sleep() {
	total := d.NextValue()
	time.Sleep(d.Unit * time.Duration(total))
}

func (d *DelayExp2) SleepWithCancel(canceled func() bool) {
	total := d.NextValue()
	for i := 0; i != total && !canceled(); i++ {
		time.Sleep(d.Unit)
	}
}
