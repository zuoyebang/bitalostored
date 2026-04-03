package collector

import (
	"github.com/gomodule/redigo/redis"
	"net/http"
)

type Instance struct {
	IP         string
	Port       string
	AdminPort  string
	Name       string
	Idc        string
	Group      string
	MachineId  int
	StoredPool *redis.Pool
	HttpClient *http.Client
}
