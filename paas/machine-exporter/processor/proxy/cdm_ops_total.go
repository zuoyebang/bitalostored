package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cdmOpsTotal struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cdmOpsTotal) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cdmOpsTotal) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cdmOpsTotal) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	cdmOpsTotalName = "cdm_ops_total"
)

func newCdmOpsTotal() *cdmOpsTotal {
	return &cdmOpsTotal{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cdmOpsTotalName,
		Help:        "Command total count",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
