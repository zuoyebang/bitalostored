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
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
)

type rotateParams struct {
	format   string
	duration time.Duration
}

const (
	MonthlyRotate = "Monthly"
	DailyRotate   = "Daily"
	HourlyRotate  = "Hourly"
)

const DefaultRotate = DailyRotate

var rotateMap = map[string]rotateParams{
	MonthlyRotate: {
		format:   ".%Y%m",
		duration: 30 * 24 * time.Hour,
	},
	DailyRotate: {
		format:   ".%Y%m%d",
		duration: 24 * time.Hour,
	},
	HourlyRotate: {
		format:   ".%Y%m%d%H",
		duration: time.Hour,
	},
}

func CheckRotation(rotation string) bool {
	_, ok := rotateMap[rotation]
	return ok
}

func getRotationParam(rotation string) rotateParams {
	param, ok := rotateMap[rotation]
	if ok {
		return param
	}

	return rotateMap[DefaultRotate]
}

func getRotateLogs(path, rotation string) *rotatelogs.RotateLogs {
	param := getRotationParam(rotation)
	rl, _ := rotatelogs.New(
		path+param.format,
		rotatelogs.WithLinkName(path),
		rotatelogs.WithMaxAge(time.Hour*24*14),
		rotatelogs.WithRotationTime(param.duration),
	)
	return rl
}
