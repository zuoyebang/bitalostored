package localcache

import (
	"fmt"
	"hash/fnv"
	"reflect"
)

var fnv32 = fnv.New32()

func hashInterface(v interface{}) uint32 {

	switch val := v.(type) {
	case string:
		fnv32.Reset()
		fnv32.Write([]byte(val))
		return fnv32.Sum32()
		// return djb33([]byte(val))
	case []byte:
		fnv32.Reset()
		fnv32.Write(val)
		return fnv32.Sum32()
		// return djb33(val)
	case int, int8, int16, int32, int64:
		return uint32(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		return uint32(reflect.ValueOf(v).Uint())
	case float32, float64:
		return uint32(reflect.ValueOf(v).Float())
	case bool:
		if val {
			return 1
		}
		return 0
	default:
		// 对于其他类型，可以转换为字符串
		fnv32.Reset()
		fnv32.Write([]byte(fmt.Sprintf("%v", v)))
		return fnv32.Sum32()
		// return djb33([]byte(fmt.Sprintf("%v", v)))
	}
}
