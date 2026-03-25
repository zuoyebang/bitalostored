// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package srv_node

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type MultiUpgradeNormalInput struct {
	ClusterId    uint               `json:"clusterId"`
	GroupNodes   []*NormalGroupNode `json:"groupNodes"`
	CosFileId    uint               `json:"packageId"`
	Operation    string             `json:"operation"`
	UpdateConfig string             `json:"updateConfig"`
	MultiType    string             `json:"multiType"`
}

type NormalGroupNode struct {
	GroupId uint   `json:"key"`
	NodeIds string `json:"value"`
}

var _ servicer.Servicer = new(MultiUpgradeNormalInput)

func (input *MultiUpgradeNormalInput) CheckParams(ctx *gin.Context) error {
	if input.GroupNodes == nil {
		return errors.New("invalid nodes")
	}
	//operation= upgrade  supervisor-stop  supervisor-start
	if input.Operation == "" {
		return errors.New("invalid operation")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	if input.MultiType != "dashboard" && input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *MultiUpgradeNormalInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	idNums := 0
	for _, groupNode := range input.GroupNodes {
		ids := strings.Split(groupNode.NodeIds, ",")
		idTmp := make([]uint, 0)
		for _, id := range ids {
			uintId, _ := strconv.ParseUint(id, 10, 64)
			idTmp = append(idTmp, uint(uintId))
		}
		idNums += len(ids)
		idArray := arrayInGroupsOf(idTmp, 20)
		for _, idA := range idArray {
			go func(nodeIds []uint) {
				for _, nodeId := range nodeIds {
					randTime := r.Int63n(4) + 3
					time.Sleep(time.Second * time.Duration(randTime))
					if input.MultiType == "dashboard" {
						time.Sleep(time.Second * 3)
						_, _ = upgradeNormalNode(nodeId, 1, input.CosFileId, input.Operation, input.UpdateConfig)
					} else {
						_, _ = upgradeNormalNode(input.ClusterId, nodeId, input.CosFileId, input.Operation, input.UpdateConfig)
					}
				}
			}(idA)
		}
	}
	return nil, nil
}

func arrayInGroupsOf(arr []uint, num int64) [][]uint {
	max := int64(len(arr))
	if max <= num {
		return [][]uint{arr}
	}
	var quantity int64
	if max%num == 0 {
		quantity = max / num
	} else {
		quantity = (max / num) + 1
	}
	var segments = make([][]uint, 0)
	var start, end, i int64
	for i = 1; i <= quantity; i++ {
		end = i * num
		if i != quantity {
			segments = append(segments, arr[start:end])
		} else {
			segments = append(segments, arr[start:])
		}
		start = i * num
	}
	return segments
}
