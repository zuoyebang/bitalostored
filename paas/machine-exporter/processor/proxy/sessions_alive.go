package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type sessionsAlive struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *sessionsAlive) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *sessionsAlive) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *sessionsAlive) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	sessionsAliveName = "sessions_alive"
)

func newSessionsAlive() *sessionsAlive {
	return &sessionsAlive{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        sessionsAliveName,
		Help:        "Average command latency",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
