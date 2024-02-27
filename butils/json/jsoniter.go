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

package json

import (
	"fmt"
	"reflect"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"

	jsoniter "github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
)

var Json = jsoniter.ConfigCompatibleWithStandardLibrary

func init() {
	extra.RegisterFuzzyDecoders()
}

var JsoniterMarshal = jsoniter.Marshal
var JsoniterUnmarshal = JsoniterUnmarshalJSON

func JsoniterUnmarshalJSON(b []byte, d interface{}) error {
	err := jsoniter.Unmarshal(b, d)
	if err != nil {
		return err
	}
	v := reflect.ValueOf(d)
	jsonField(v)
	return nil
}

func jsonField(v reflect.Value) {
	t := v.Type()
	switch t.Kind() {
	case reflect.Ptr:
		if !v.Elem().IsValid() {
			return
		}
		fn := v.Elem().NumField()
		for i := 0; i < fn; i++ {
			if v.Elem().Kind() == reflect.Struct {
				if name, ok := t.Elem().Field(i).Tag.Lookup("jsonfield"); ok {
					if !v.Elem().FieldByName(name).IsValid() {
						panic(fmt.Errorf("jsonfield '%s' not exists", name))
					}
					receiver := reflect.New(v.Elem().Field(i).Type())
					if vv, ok := v.Elem().FieldByName(name).Interface().(string); ok && len(vv) > 2 {
						err := jsoniter.Unmarshal(unsafe2.ByteSlice(vv), receiver.Interface())
						if err == nil {
							v.Elem().Field(i).Set(receiver.Elem())
						}
					}
				}
				jsonField(v.Elem().Field(i))
			}
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			jsonField(v.MapIndex(k))
		}
	case reflect.Slice:
		for j := 0; j < v.Len(); j++ {
			jsonField(v.Index(j))
		}
	}
}
