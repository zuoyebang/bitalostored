package internal

type MetricList = map[string]float64
type MachinePortMetric = map[string]map[string]MetricList

type NodeCpuLimit struct {
	IP             string
	Port           string
	CpuThrottledNr float64
}
