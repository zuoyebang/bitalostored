package config

import (
	"encoding/json"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/webclient"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	ServerAddress     string `toml:"serverAddress"`
	DeployPath        string `toml:"deployPath"`
	AgentPath         string `toml:"agentPath"`
	ProxyLogPath      string `toml:"proxyLogPath"`
	WebPort           string `toml:"webPort"`
	DbMode            string `toml:"dbMode"`
	SqlitePath        string `toml:"sqlitePath"`
	DisableLogCollect bool   `toml:"disableLogCollect"`
	Area              string `toml:"area"`
	MachineId         uint
	IP                string
	Cos               CosConfig   `toml:"cos"`
	Mysql             MysqlConfig `toml:"db"`
	BsClient          MysqlConfig `toml:"bsdb"`
	Timezone          *time.Location
}

type CosConfig struct {
	Ak  string
	Sk  string
	Url string
}

var C *Config

type MysqlConfig struct {
	Server   string
	Port     string
	Username string
	Password string
	Database string
}

func (c *Config) Validate() error {
	if c.ServerAddress == "" || c.DeployPath == "" {
		return errors.New("incorrect config")
	}
	if len(c.ProxyLogPath) <= 0 {
		return errors.New("proxyLogPath config error")
	}
	if machineId := registerMachineId(); machineId <= 0 {
		return errors.New("wrong machineId")
	} else {
		C.MachineId = machineId
	}
	if c.AgentPath == "" {
		c.AgentPath = "/home/homework/bitalos-paas/bitalos-agent"
	}
	c.AgentPath = strings.TrimRight(c.AgentPath, "/")
	logs.Infof("config detail:%+v", c)
	return nil
}

func SetConfig(confPath string) error {
	file, err := os.Open(confPath)
	if err != nil {
		return errors.Trace(err)
	}
	defer file.Close()

	d := toml.NewDecoder(file)
	err = d.Decode(&C)
	if err != nil {
		return errors.Trace(err)
	}
	logs.Infof("config detail:%+v", C)
	//C.Timezone, _ = time.LoadLocation("Asia/Shanghai")
	C.Timezone = time.FixedZone("UTC", 8*3600)
	return C.Validate()
}

func GetPaaSAddress(path string) string {
	return C.ServerAddress + path
}

func GetDeployPath() string {
	return C.DeployPath
}

func GetWebPort() string {
	return ":" + C.WebPort
}

func getIdc(ip string) (idc string) {
	if len(ip[:]) <= 8 {
		idc = ""
	} else if ip[0:4] == "192." || ip[0:5] == "10.14" {
		idc = "baidu"
	} else if ip[0:4] == "172." {
		idc = "tencent"
	} else if ip[0:6] == "10.106" {
		idc = "txcloud"
	} else if ip[0:3] == "10." {
		idc = "ali"
	}
	return idc
}

func registerMachineId() (machineId uint) {
	C.IP = webclient.GetLocalIp()
	data, err := webclient.PostPaaS(GetPaaSAddress(def.URL_REGISTER), map[string]interface{}{
		"ip": C.IP,
	})
	if err != nil {
		logs.Warn("register machine id failed.err:", err)
		return 0
	}
	var m MachineRegister
	err = json.Unmarshal(data, &m)
	if m.Status != 0 || err != nil {
		logs.Warn("register machine id failed.registerInfo:", m, "err:", err, "data;", string(data))
		return 0
	}
	return m.Data
}

type MachineRegister struct {
	webclient.PaaSResponse
	Data uint `json:"data"`
}
