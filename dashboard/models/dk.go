package models

import "fmt"

type DkItem struct {
	Key       string   `json:"key"`
	ShardNum  int      `json:"shardNum"`
	DataType  string   `json:"dataType"`
	GroupKeys []string `json:"groupKeys"`
}

var (
	DkMainKey = []byte("77c44df457908f56")
)

func (d *DkItem) Encode() []byte {
	return jsonEncode(d)
}

func (d *DkItem) GenerateGroupKeys() {
	gks := make([]string, 0, d.ShardNum)
	for i := 0; i < d.ShardNum; i++ {
		gk := EncodeDkGroupKey(d.Key, i)
		gks = append(gks, gk)
	}
	d.GroupKeys = gks
}

func EncodeDkGroupKey(dkKey string, shardNum int) string {
	return fmt.Sprintf("%s_%s_%d", dkKey, DkMainKey, shardNum)
}
