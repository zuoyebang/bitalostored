package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type diskRaftNodehostSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *diskRaftNodehostSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *diskRaftNodehostSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *diskRaftNodehostSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	diskRaftNodehostSizeName = "disk_raft_nodehost_size"
)

func newDiskRaftNodehostSize() *diskRaftNodehostSize {
	return &diskRaftNodehostSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        diskRaftNodehostSizeName,
		Help:        "disk_raft_nodehost_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
