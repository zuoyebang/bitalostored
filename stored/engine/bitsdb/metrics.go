package bitsdb

import (
	"bytes"
	"encoding/json"
)

type MetricsInfo struct {
}

func (i MetricsInfo) String() string {
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

type DBDiskDetail struct {
	BitupleDisk uint64
	BitpageDisk uint64
	BithashDisk uint64
}

type DBStats struct {
	VmTableFlushLastCost  int64
	VmTableFlushAvgCost   int64
	MemTableFlushLastCost int64
	MemTableFlushAvgCost  int64
}

type DBDiskInfo struct {
	PBDbInfo string `json:"pb_db_info"`
}

func (d *DBDiskInfo) Marshal() []byte {
	var buf bytes.Buffer
	buf.WriteString(d.PBDbInfo + "\n")
	return buf.Bytes()
}
