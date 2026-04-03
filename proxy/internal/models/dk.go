package models

type DkItem struct {
	Key      string `json:"key"`
	ShardNum uint32 `json:"shardNum"`
	DataType string `json:"dataType"`
}

func (d *DkItem) CheckDt() bool {
	if d.DataType != "hash" && d.DataType != "set" {
		return false
	}
	return true
}
