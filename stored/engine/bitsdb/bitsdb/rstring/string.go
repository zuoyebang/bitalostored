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

package rstring

import (
	"errors"
	"strconv"

	"github.com/shopspring/decimal"
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
)

type StringObject struct {
	base.BaseObject
}

func NewStringObject(baseDb *base.BaseDB, cfg *dbconfig.Config) *StringObject {
	so := &StringObject{
		BaseObject: base.NewBaseObject(baseDb, cfg, btools.STRING),
	}
	return so
}

func (so *StringObject) Close() {
	so.BaseObject.Close()
}

func (so *StringObject) incr(key []byte, khash uint32, delta int64) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	var n int64
	ek, ekCloser := base.EncodeMetaKey(key, khash)
	val, timestamp, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	n, err = btools.StrInt64(val, err)
	if err != nil {
		return 0, errors.New("ERR value is not an integer or out of range")
	}

	n += delta
	return n, so.setValueForString(ek, extend.FormatInt64ToSlice(n), timestamp)
}

func (so *StringObject) incrFloat(key []byte, khash uint32, delta float64) (float64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	var n float64
	ek, ekCloser := base.EncodeMetaKey(key, khash)
	val, timestamp, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	n, err = btools.StrFloat64(val, err)
	if err != nil {
		return 0, err
	}

	f, _ := decimal.NewFromFloat(n).Add(decimal.NewFromFloat(delta)).Float64()
	return f, so.setValueForString(ek, []byte(strconv.FormatFloat(f, 'f', -1, 64)), timestamp)
}

func (so *StringObject) getValueForString(key []byte) ([]byte, uint64, func(), error) {
	eval, closer, err := so.BaseDb.GetMeta(key)
	if eval == nil || err != nil {
		return nil, 0, closer, err
	}

	dt, timestamp, val := base.DecodeMetaValueForString(eval)
	if dt != so.DataType {
		log.Errorf("getValueForString dataType notmatch key:%s exp:%d act:%d", string(key), so.DataType, dt)
		return nil, 0, closer, errn.ErrWrongType
	}

	return val, timestamp, closer, nil
}

func (so *StringObject) getValueCheckAliveForString(key []byte) ([]byte, uint64, func(), error) {
	val, timestamp, closer, err := so.getValueForString(key)
	if val == nil || err != nil {
		return nil, 0, closer, err
	}

	if timestamp > 0 {
		nowTime := tclock.GetTimestampMilli()
		if int64(timestamp) <= nowTime {
			return nil, 0, closer, nil
		}
	}

	return val, timestamp, closer, nil
}

func (so *StringObject) setValueForString(ek, value []byte, timestamp uint64) (err error) {
	var metaValue [base.MetaStringValueLen]byte
	base.EncodeMetaDbValueForString(metaValue[:], timestamp)
	vlen := base.MetaStringValueLen + len(value)
	return so.SetMetaDataByValues(ek, vlen, metaValue[:], value)
}

func (so *StringObject) setMultiValueForString(ek, oldValue, value []byte, timestamp uint64) (err error) {
	var metaValue [base.MetaStringValueLen]byte
	base.EncodeMetaDbValueForString(metaValue[:], timestamp)
	vlen := base.MetaStringValueLen + len(oldValue) + len(value)
	return so.SetMetaDataByValues(ek, vlen, metaValue[:], oldValue, value)
}
