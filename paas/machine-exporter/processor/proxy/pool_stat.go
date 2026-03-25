package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type poolStat struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *poolStat) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *poolStat) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *poolStat) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	poolActiveCountName = "pool_active_count"
	poolIdleCountName   = "pool_idle_count"
)

func newPoolActiveCount() *poolStat {
	return &poolStat{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        poolActiveCountName,
		Help:        "Available pool connections",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}

func newPoolIdleCount() *poolStat {
	return &poolStat{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        poolIdleCountName,
		Help:        "Idle pool connections",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
