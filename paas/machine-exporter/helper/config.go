package helper

import "errors"

type MeConf struct {
	Env      Env      `toml:"env" json:"env"`
	Database Database `toml:"database" json:"database"`
}

func (c *MeConf) Validate() error {
	if len(c.Env.MetricPath) <= 0 || len(c.Env.Address) <= 0 {
		return errors.New("invalid port config")
	}
	return nil
}

type Env struct {
	MetricPath string `toml:"metric_path" json:"metricPath"`
	Address    string `toml:"address" json:"address"`
}

type Database struct {
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
	Hostport string `toml:"hostport" json:"hostport"`
	Database string `toml:"database" json:"database"`
}
