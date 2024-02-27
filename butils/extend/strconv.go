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

package extend

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/numeric"
)

func ToString(v interface{}) (string, error) {
	switch v := v.(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int:
		return strconv.Itoa(v), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func ToFloat64(v interface{}) (float64, error) {
	switch v := v.(type) {
	case float64:
		return v, nil
	case uint64:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("ToFloat64: unknown supported type %T", v)
	}
}

func ToFloat64frombits(v interface{}) (float64, error) {
	switch v := v.(type) {
	case float64:
		return v, nil
	case uint64:
		return numeric.Uint64ToFloat64(v), nil
	case int64:
		return numeric.Int64ToFloat64(v), nil
	case int:
		return numeric.Int64ToFloat64(int64(v)), nil
	case string:
		if strings.Contains(v, ".") {
			return strconv.ParseFloat(v, 64)
		}
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return numeric.Int64ToFloat64(int64(i)), nil
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("ToFloat64frombits: unknown supported type %T", v)
	}
}

func ToUint64(v interface{}) (uint64, error) {
	switch v := v.(type) {
	case uint64:
		return v, nil
	case float64:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("ToInt: unknown supported type %T", v)
	}
}

func ToInt(v interface{}) (int, error) {
	switch v := v.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case uint64:
		return int(v), nil
	case int64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("ToInt: unknown supported type %T", v)
	}
}

func ToBool(v interface{}) (bool, error) {
	switch v := v.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case float64:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	default:
		return false, fmt.Errorf("ToInt: unknown supported type %T", v)
	}
}
