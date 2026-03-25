package collector

import (
	"strconv"
	"strings"
)

func ParseServerInfo(info string, isMaster bool) map[string]float64 {
	res := make(map[string]float64, 0)
	split := strings.Split(info, "\n")
	res["role"] = 0
	for _, v := range split {
		if info := strings.Split(v, ":"); len(info) == 2 {
			info[1] = strings.Trim(info[1], " ")
			if info[0] == "role" {
				if info[1] == "master" {
					res["role"] = 1
				} else {
					res["role"] = 0
				}
				continue
			}

			float, err := strconv.ParseFloat(info[1], 64)
			if err == nil {
				res[info[0]] = float
			} else {
				bytes, err := ToBytes(info[1])
				if err == nil {
					res[info[0]] = float64(bytes)
				}
			}
		}
	}
	if isMaster {
		res["role"] = 1
	}
	return res
}

func ParseInfo(info string) map[string]float64 {
	res := make(map[string]float64, 0)
	split := strings.Split(info, "\n")
	for _, v := range split {
		if info := strings.Split(v, ":"); len(info) == 2 {
			info[1] = strings.Trim(info[1], " ")
			if info[0] == "role" {
				if info[1] == "master" {
					res["role"] = 1
				} else {
					res["role"] = 0
				}
				continue
			}

			float, err := strconv.ParseFloat(info[1], 64)
			if err == nil {
				res[info[0]] = float
			} else {
				bytes, err := ToBytes(info[1])
				if err == nil {
					res[info[0]] = float64(bytes)
				}
			}
		}
	}
	return res
}

type ProxyStatsResp struct {
	Status int `json:"status"`
	Data   struct {
		CdmOps struct {
			Total       int64 `json:"total"`
			Fails       int   `json:"fails"`
			Periodfails int   `json:"periodfails"`
			QPS         int   `json:"qps"`
			Cmd         []struct {
				Opstr        string `json:"opstr"`
				Calls        int    `json:"calls"`
				Usecs        int    `json:"usecs"`
				UsecsPercall int    `json:"usecs_percall"`
				Fails        int    `json:"fails"`
				Periodfails  int    `json:"periodfails"`
			} `json:"cmd"`
		} `json:"cdm_ops"`
	} `json:"data"`
}

func ParseProxyStatInfo(info ProxyStatsResp) map[string]float64 {
	res := make(map[string]float64, 0)
	if len(info.Data.CdmOps.Cmd) <= 0 {
		return res
	}
	for _, s := range info.Data.CdmOps.Cmd {
		res["cmd_opstr_qps"+s.Opstr] = float64(s.Calls)
		res["cmd_opstr_cost"+s.Opstr] = float64(s.UsecsPercall)
	}
	return res
}
