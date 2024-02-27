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

package bitskv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	kv_bitable "github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv/bitable"
	kv_bitalosdb "github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv/bitalosdb"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type IterOptions = kv.IteratorOptions

var nilIterOptions = &IterOptions{
	LowerBound: nil,
	UpperBound: nil,
	IsAll:      true,
	SlotId:     0,
	KeyHash:    0,
}

var newBitableKVStrore = kv_bitable.NewKVStore
var newBitalosdbKVStrore = kv_bitalosdb.NewKVStore

type DB struct {
	DebugInfo DBDebugInfo
	metaDb    kv.IKVStore
	expireDb  kv.IKVStore
	dataDb    kv.IKVStore
	indexDb   kv.IKVStore
	cfg       *dbconfig.Config
	sPath     string
	dt        btools.DataType
}

func NewBaseDB(cfg *dbconfig.Config) (*DB, error) {
	var err error
	pdb := &DB{
		DebugInfo: DBDebugInfo{},
		sPath:     cfg.DBPath,
		cfg:       cfg,
		dataDb:    nil,
		indexDb:   nil,
	}

	metaDbPath := filepath.Join(pdb.sPath, kv.DB_TYPE_DIR_META)
	pdb.metaDb, err = newBitalosdbKVStrore(metaDbPath, pdb.cfg, btools.NoneType, kv.DB_TYPE_META)
	if err != nil {
		return nil, err
	}

	expireDbPath := filepath.Join(pdb.sPath, kv.DB_TYPE_DIR_EXPIRE)
	pdb.expireDb, err = newBitableKVStrore(expireDbPath, pdb.cfg, btools.NoneType, kv.DB_TYPE_EXPIRE)
	if err != nil {
		return nil, err
	}

	return pdb, nil
}

func NewDataDB(sPath string, dataType btools.DataType, cfg *dbconfig.Config) (*DB, error) {
	var err error
	pdb := &DB{
		DebugInfo: DBDebugInfo{},
		sPath:     sPath,
		cfg:       cfg,
		dt:        dataType,
		metaDb:    nil,
		expireDb:  nil,
		dataDb:    nil,
		indexDb:   nil,
	}

	dataDbPath := filepath.Join(sPath, dataType.String())
	pdb.dataDb, err = newBitalosdbKVStrore(dataDbPath, pdb.cfg, dataType, kv.DB_TYPE_DATA)
	if err != nil {
		return nil, err
	}

	if dataType == btools.ZSET {
		indexDbPath := filepath.Join(sPath, dataType.String()) + kv.DB_TYPE_DIR_INDEX
		pdb.indexDb, err = newBitalosdbKVStrore(indexDbPath, pdb.cfg, dataType, kv.DB_TYPE_INDEX)
		if err != nil {
			return nil, err
		}
	}

	return pdb, nil
}

func (db *DB) Close() {
	if db.dataDb != nil {
		db.dataDb.Close()
		db.dataDb = nil
	}
	if db.indexDb != nil {
		db.indexDb.Close()
		db.indexDb = nil
	}
	if db.metaDb != nil {
		db.metaDb.Close()
		db.metaDb = nil
	}
	if db.expireDb != nil {
		db.expireDb.Close()
		db.expireDb = nil
	}
}

func (db *DB) String() string {
	return db.sPath
}

func (db *DB) GetDataType() string {
	return db.dt.String()
}

func (db *DB) IsNotFound(err error) bool {
	return err == kv_bitalosdb.ErrNotFound || err == kv_bitable.ErrNotFound
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.dataDb.Get(key)
}

func (db *DB) GetData(key []byte) ([]byte, func(), error) {
	return db.dataDb.GetPools(key)
}

func (db *DB) GetMeta(key []byte) ([]byte, func(), error) {
	return db.metaDb.GetPools(key)
}

func (db *DB) GetExpire(key []byte) ([]byte, error) {
	return db.expireDb.Get(key)
}

func (db *DB) GetIndex(key []byte) ([]byte, error) {
	return db.indexDb.Get(key)
}

func (db *DB) IsExistData(key []byte) (bool, error) {
	return db.dataDb.IsExist(key)
}

func (db *DB) IsExistExpire(key []byte) (bool, error) {
	return db.expireDb.IsExist(key)
}

func (db *DB) Put(key []byte, value []byte) error {
	return db.dataDb.Set(key, value)
}

func (db *DB) PutMeta(key []byte, value []byte) error {
	return db.metaDb.Set(key, value)
}

func (db *DB) Delete(key []byte) error {
	return db.dataDb.Delete(key)
}

func (db *DB) FlushDB() error {
	return db.dataDb.Flush()
}

func (db *DB) FlushMeta() error {
	return db.metaDb.Flush()
}

func (db *DB) GetAllDB() (dbs []kv.IKVStore) {
	if db.metaDb != nil {
		dbs = append(dbs, db.metaDb)
	}
	if db.expireDb != nil {
		dbs = append(dbs, db.expireDb)
	}
	if db.dataDb != nil {
		dbs = append(dbs, db.dataDb)
	}
	if db.indexDb != nil {
		dbs = append(dbs, db.indexDb)
	}
	return
}

func (db *DB) Flush() {
	if db.metaDb != nil {
		db.metaDb.Flush()
	}
	if db.expireDb != nil {
		db.expireDb.Flush()
	}
	if db.dataDb != nil {
		db.dataDb.Flush()
	}
	if db.indexDb != nil {
		db.indexDb.Flush()
	}
}

func (db *DB) CompactDB() {
	if db.dataDb != nil {
		db.dataDb.Compact(0)
	}
	if db.expireDb != nil {
		db.expireDb.Compact(0)
	}
	if db.metaDb != nil {
		db.metaDb.Compact(0)
	}
	if db.indexDb != nil {
		db.indexDb.Compact(0)
	}
}

func (db *DB) GetMetaDbDebugInfo() {
	db.DebugInfo.PBDbInfo = db.metaDb.DebugInfo()
}

func (db *DB) GetDataDbDebugInfo() {
	db.DebugInfo.PBDbInfo = db.dataDb.DebugInfo()
}

func (db *DB) GetIndexDbDebugInfo() {
	db.DebugInfo.PBDbInfo = db.indexDb.DebugInfo()
}

func (db *DB) GetCacheInfo() string {
	return db.metaDb.CacheInfo()
}

func (db *DB) NewIterator(o *IterOptions) *Iterator {
	if o == nil {
		o = nilIterOptions
	}
	if o.KeyHash > 0 {
		o.SlotId = uint32(utils.GetSlotId(o.KeyHash))
	}
	it := &Iterator{
		it: db.dataDb.NewIter(o),
	}
	return it
}

func (db *DB) NewIteratorMeta(o *IterOptions) *Iterator {
	if o == nil {
		o = nilIterOptions
	}
	if o.KeyHash > 0 {
		o.SlotId = uint32(utils.GetSlotId(o.KeyHash))
	}
	it := &Iterator{
		it: db.metaDb.NewIter(o),
	}
	return it
}

func (db *DB) NewIteratorIndex(o *IterOptions) *Iterator {
	if o == nil {
		o = nilIterOptions
	}
	if o.KeyHash > 0 {
		o.SlotId = uint32(utils.GetSlotId(o.KeyHash))
	}
	it := &Iterator{
		it: db.indexDb.NewIter(o),
	}
	return it
}

func (db *DB) NewIteratorExpire(o *IterOptions) *Iterator {
	if o == nil {
		o = nilIterOptions
	}
	it := new(Iterator)
	it.it = db.expireDb.NewIter(o)
	return it
}

func (db *DB) SetCheckpointLock(v bool) {
	if db.dataDb != nil {
		db.dataDb.SetCheckpointLock(v)
	}
	if db.metaDb != nil {
		db.metaDb.SetCheckpointLock(v)
	}
	if db.indexDb != nil {
		db.indexDb.SetCheckpointLock(v)
	}
	if db.expireDb != nil {
		db.expireDb.SetCheckpointLock(v)
	}
}

func (db *DB) SetCheckpointHighPriority(v bool) {
	if db.dataDb != nil {
		db.dataDb.SetCheckpointHighPriority(v)
	}
	if db.metaDb != nil {
		db.metaDb.SetCheckpointHighPriority(v)
	}
	if db.indexDb != nil {
		db.indexDb.SetCheckpointHighPriority(v)
	}
	if db.expireDb != nil {
		db.expireDb.SetCheckpointHighPriority(v)
	}
}

func (db *DB) Checkpoint(sDir string) error {
	if db.dataDb != nil {
		dataPath := sDir
		log.Infof("checkpoint dataDb dbpath:%s", dataPath)
		if err := db.dataDb.Checkpoint(dataPath); err != nil {
			return fmt.Errorf("checkpoint dataDb path:%s error:%s", dataPath, err)
		}
	}
	if db.indexDb != nil {
		indexPath := sDir + kv.DB_TYPE_DIR_INDEX
		log.Infof("checkpoint indexDb dbpath:%s", indexPath)
		if err := db.indexDb.Checkpoint(indexPath); err != nil {
			return fmt.Errorf("checkpoint indexDb path:%s error:%s", indexPath, err)
		}
	}
	if db.metaDb != nil {
		metaPath := filepath.Join(sDir, kv.DB_TYPE_DIR_META)
		log.Infof("checkpoint metaDb dbpath:%s", metaPath)
		if err := db.metaDb.Checkpoint(metaPath); err != nil {
			return fmt.Errorf("checkpoint metaDb path:%s error:%s", metaPath, err)
		}
	}
	if db.expireDb != nil {
		expirePath := filepath.Join(sDir, kv.DB_TYPE_DIR_EXPIRE)
		log.Infof("checkpoint expireDb dbpath:%s", expirePath)
		if err := db.expireDb.Checkpoint(expirePath); err != nil {
			return fmt.Errorf("checkpoint expireDb path:%s error:%s", expirePath, err)
		}
	}
	return nil
}

func (db *DB) DataStats() ForestInfo {
	if db.dataDb == nil {
		return ForestInfo{}
	}
	return ForestInfo(db.dataDb.ForestInfo())
}

func (db *DB) MetaStats() ForestInfo {
	if db.metaDb == nil {
		return ForestInfo{}
	}
	return ForestInfo(db.metaDb.ForestInfo())
}

func (db *DB) ExpireStats() ForestInfo {
	if db.expireDb == nil {
		return ForestInfo{}
	}
	return ForestInfo(db.expireDb.ForestInfo())
}

func (db *DB) IndexStats() ForestInfo {
	if db.indexDb == nil {
		return ForestInfo{}
	}
	return ForestInfo(db.indexDb.ForestInfo())
}

type ForestInfo kv.ForestInfo

func (i ForestInfo) String() string {
	s, err := json.Marshal(i)
	if err != nil {
		return ""
	} else {
		return string(s)
	}
}

type DBDebugInfo struct {
	PBDbInfo string `json:"pb_db_info"`
}

func (d *DBDebugInfo) Marshal() []byte {
	var buf bytes.Buffer
	buf.WriteString(d.PBDbInfo + "\n")
	return buf.Bytes()
}
