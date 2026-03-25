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

package srv_machine

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sync"
)

type MachineReplicateInput struct {
	Ip    string `json:"ip"`
	Token string `json:"token"`
}

var _ servicer.Servicer = new(MachineReplicateInput)

func (input *MachineReplicateInput) CheckParams(ctx *gin.Context) error {
	if len(input.Ip) <= 0 {
		return errors.New("empty ip")
	}
	t := math2.GetMd5(input.Ip)
	if t != input.Token {
		return errors.New("token invalid")
	}
	return nil
}

func (input *MachineReplicateInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	minfo, err := tbl_machine.GetMachineInfo(input.Ip)
	if err != nil {
		return nil, err
	}
	if minfo.ID <= 0 {
		return nil, errors.New("invalid ip")
	}
	if minfo.IsVirtual == def.MACHINE_VIRTUAL {
		return nil, errors.New("The virtual machine proxy cannot be used here.")
	}
	nodeList, err := tbl_node.GetMachineAllNodes(minfo.ID, []uint{def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX}, 0)
	if err != nil {
		log.Warn("get machine nodelist failed.err:", err)
		return nil, err
	}
	if len(nodeList) <= 0 {
		return nil, errors.New("No deployment nodes deployed")
	}
	if err := dashboard.SetDashboardCookie(ctx); err != nil {
		return nil, errors.New("Failed to set the dh cookie")
	}
	clusterMap := make(map[uint]string)
	clusterIds := make([]uint, 0)
	for i := 0; i < len(nodeList); i++ {
		if _, ok := clusterMap[nodeList[i].ClusterId]; !ok {
			clusterIds = append(clusterIds, nodeList[i].ClusterId)
		}
		clusterMap[nodeList[i].ClusterId] = ""
	}
	clusterList, err := tbl_cluster.GetListByIds(clusterIds)
	if err != nil {
		return nil, errors.New("Failed to obtain the list of clusters")
	}
	for i := 0; i < len(clusterList); i++ {
		clusterMap[clusterList[i].Id] = clusterList[i].Name
	}
	results := make([]string, len(nodeList))
	var wg sync.WaitGroup
	for i := 0; i < len(nodeList); i++ {
		wg.Add(1)
		go func(nodeInfo *tbl_node.Node, index int) {
			defer wg.Done()
			address := fmt.Sprintf("%s:%d", input.Ip, nodeInfo.ServicePort)

			err = dashboard.ReplicaNode(ctx, address, clusterMap[nodeInfo.ClusterId], 0, nodeInfo.GroupId)
			if err != nil {
				e := fmt.Sprintf("%s %s replica failed，error: %s", clusterMap[nodeInfo.ClusterId], address, err.Error())
				results[index] = e
				return
			} else {
				err = dashboard.SyncGroup(ctx, clusterMap[nodeInfo.ClusterId], nodeInfo.GroupId)
				if err != nil {
					e := fmt.Sprintf("%s %s sync failed，error: %s", clusterMap[nodeInfo.ClusterId], address, err.Error())
					results[index] = e
					return
				}
			}
			e := fmt.Sprintf("%s %s replica success", clusterMap[nodeInfo.ClusterId], address)
			results[index] = e
		}(nodeList[i], i)
	}
	wg.Wait()
	newResults := make([]string, 0)
	for _, r := range results {
		if len(r) <= 0 {
			continue
		}
		newResults = append(newResults, r)
	}
	return newResults, nil
}
