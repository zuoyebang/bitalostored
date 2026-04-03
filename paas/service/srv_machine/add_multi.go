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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
)

type AddMultiInput struct {
	IP        string `json:"ip"`
	IDC       string `json:"idc"`
	Budget    string `json:"budget"`
	CpuTotal  int    `json:"cpuTotal"`
	CpuSetMax int    `json:"cpuSetMax"`
	IsVirtual int    `json:"isVirtual"`
}

var _ servicer.Servicer = new(AddMultiInput)

func (input *AddMultiInput) CheckParams(ctx *gin.Context) error {
	if input.IP == "" {
		return errors.New("invalid IP")
	}
	return nil
}

func (input *AddMultiInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	ips := strings.Split(input.IP, "\n")
	formatIps := make([]string, 0, len(ips))
	for _, ip := range ips {
		tmpIp := strings.TrimSpace(ip)
		if len(tmpIp) > 0 {
			formatIps = append(formatIps, tmpIp)
		}
	}
	exsitList, err := tbl_machine.GetMachinesByIpList(formatIps)
	if err != nil {
		return nil, err
	}
	if len(exsitList) > 0 {
		return nil, errors.New("ip exists " + exsitList[0].IP)
	}

	for _, ip := range formatIps {
		_, err = tbl_machine.Register(ip, input.IDC, input.Budget, "", input.CpuTotal, input.CpuSetMax, input.IsVirtual)
		if err != nil {
			log.Errorf("Failed to register machine: %s err: %v", ip, err)
		}
	}

	// var wg sync.WaitGroup
	// for _, ip := range formatIps {
	// 	wg.Add(1)
	// 	go func(ip string) {
	// 		defer wg.Done()
	// 		cmd := fmt.Sprintf("ssh homework@%s hostname", ip)
	// 		hostName, err := exec.Command("bash", "-c", cmd).Output()
	// 		if err != nil {
	// 			log.Warnf("cmd: %s err, err = %v", cmd, err)
	// 			return
	// 		}
	// 		_, err = tbl_machine.Register(ip, input.IDC, input.Budget, string(hostName), input.CpuTotal, input.CpuSetMax)
	// 		if err != nil {
	// 			log.Errorf("Failed to register machine: %s err: %v", ip, err)
	// 		}
	// 	}(ip)
	// }
	// wg.Wait()
	return len(formatIps), nil
}
