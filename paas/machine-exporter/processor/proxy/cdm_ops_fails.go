package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cdmOpsFails struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cdmOpsFails) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cdmOpsFails) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cdmOpsFails) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	cdmOpsFailsName = "cdm_ops_fails"
)

func newCdmOpsFails() *cdmOpsFails {
	return &cdmOpsFails{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cdmOpsFailsName,
		Help:        "Command failure count",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
