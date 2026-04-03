package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cdmOpsQps struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cdmOpsQps) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cdmOpsQps) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cdmOpsQps) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	cdmOpsQpsName = "cdm_ops_qps"
)

func newCdmOpsQps() *cdmOpsQps {
	return &cdmOpsQps{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cdmOpsQpsName,
		Help:        "Command QPS",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
