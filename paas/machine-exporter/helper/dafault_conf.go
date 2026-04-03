package helper

import (
	"github.com/BurntSushi/toml"
	"log"
)

var defaultConf *MeConf

func GetConf() MeConf {
	return *defaultConf
}

func SetConf(configPath string) error {
	_, err := toml.DecodeFile(configPath, &defaultConf)
	if err != nil {
		return err
	}
	log.Printf("config detail:%+v", defaultConf)
	return defaultConf.Validate()
}
