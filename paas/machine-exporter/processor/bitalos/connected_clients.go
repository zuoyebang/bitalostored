package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type connectedClients struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *connectedClients) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *connectedClients) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *connectedClients) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	connectedClientsName = "connected_clients"
)

func newConnectedClients() *connectedClients {
	return &connectedClients{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        connectedClientsName,
		Help:        "Client connections",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
