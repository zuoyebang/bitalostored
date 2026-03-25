package collector

import (
	"github.com/prometheus/client_golang/prometheus"
)

type ServerProcessor interface {
	Process(machine string, name string, port string, idc string, group string, v float64)
	prometheus.Collector
}

type ProxyProcessor interface {
	Process(machine string, name string, port string, idc string, v float64)
	prometheus.Collector
}
