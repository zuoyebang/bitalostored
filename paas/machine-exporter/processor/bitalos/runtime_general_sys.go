package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type runtimeGeneralSys struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *runtimeGeneralSys) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *runtimeGeneralSys) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *runtimeGeneralSys) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	runtimeGeneralSysName = "runtime_general_sys"
)

func newRuntimeGeneralSys() *runtimeGeneralSys {
	return &runtimeGeneralSys{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        runtimeGeneralSysName,
		Help:        "",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
