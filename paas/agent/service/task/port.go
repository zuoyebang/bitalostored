package task

import (
	"encoding/json"
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/webclient"
	"net"
)

func GetServicePort(begin, end int) int {
	for i := begin; i < end; i++ {
		tcp, e := net.Dial("tcp", fmt.Sprintf(":%d", i))
		if e == nil {
			tcp.Close()
			continue
		}
		if buf, err := webclient.PostPaaS(config.GetPaaSAddress(def.URL_HOSTPORT), map[string]interface{}{
			"machineId": config.C.MachineId, "ip": config.C.IP, "port": i,
		}); err != nil {
			logs.Warn("get port: ", err)
		} else {
			logs.Info("res:", string(buf))
			res := HostPort{}
			if err := json.Unmarshal(buf, &res); err != nil {
				logs.Warn("get port err: ", err)
				continue
			}

			if res.Status == 0 && res.Data > 0 {
				logs.Info("getport", i)
				return i
			}
		}
	}
	return 0
}

type HostPort struct {
	webclient.PaaSResponse
	Data uint `json:"data,omitempty"`
}
