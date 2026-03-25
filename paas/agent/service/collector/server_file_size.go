package collector

import (
	"github.com/zuoyebang/bitalostored/paas/agent/dao/collect"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"os"
	"path"
	"strings"
	"time"
)

func collectVtSize(dpInfo *NodeDeployInfoResp) {
	data := dpInfo.Data
	logs.Info("start collect vt size")
	d := make([]*collect.ServerActionLogModel, 0)
	now := time.Now().Unix()
	for _, sl := range data.ServerLog {
		vtPath := path.Join(sl.Path, "bitalosdb", "bitalos", "bitvector")
		if _, err := os.Stat(vtPath); os.IsNotExist(err) {
			continue
		}
		bituplePath := path.Join(vtPath, "bituple*/*")
		res, err := Command("sh", "-c", "du -sb "+bituplePath)
		if err != nil {
			logs.Errorf("du dir %s err %v", bituplePath, err)
			continue
		}
		for line := range strings.Lines(res) {
			l := strings.TrimRight(line, "\r\n")
			sp := strings.Split(l, "\t")
			if len(sp) != 2 {
				logs.Warnf("du -sb err dir:%s", line)
				continue
			}
			if len(sp[0]) <= 0 {
				continue
			}
			sm := &collect.ServerActionLogModel{
				Ip:            data.LocalIp,
				ClusterName:   sl.ClusterName,
				Port:          sl.Port,
				ActionEndTime: now * 1000,
				CreateTime:    now,
			}
			sm.ActionSize = sp[0]
			filePath := sp[1]
			fsp := strings.Split(filePath, "/")
			if len(fsp) < 2 {
				logs.Warnf("split by / error path:%s", filePath)
				continue
			}
			bitupleName := fsp[len(fsp)-2]
			sm.DbType = "bituple"
			vtName := fsp[len(fsp)-1]
			sm.Job = path.Join(bitupleName, vtName)
			dsp := strings.Split(vtName, ".")
			if len(dsp) < 2 {
				logs.Warnf("split by . error dir:%s fileName:%s", line, vtName)
				continue
			}
			switch dsp[1] {
			case "vti":
				sm.ActionType = def.ActionTypeBitupleVti
			case "vtk":
				sm.ActionType = def.ActionTypeBitupleVtk
			case "vtm":
				sm.ActionType = def.ActionTypeBitupleVtm
			case "vtv":
				sm.ActionType = def.ActionTypeBitupleVtv
			default:
				logs.Warnf("vtname error dir:%s fileName:%s", bitupleName, vtName)
				continue
			}
			d = append(d, sm)
			if len(d) >= FlushDbLen {
				service := collect.NewServerActionLogService()
				_ = service.MultiInsertLog(d)
				d = d[0:0]
				time.Sleep(time.Second)
			}
		}
	}
	if len(d) > 0 {
		service := collect.NewServerActionLogService()
		_ = service.MultiInsertLog(d)
	}
}
