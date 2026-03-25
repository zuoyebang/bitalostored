package collector

var (
	Namespace          = "stored"
	SubSystem          = "machine"
	ServerLabels       = []string{"machine", "name", "port", "idc", "group"}
	ProxyLabels        = []string{"machine", "name", "port", "idc"}
	ProxyLabelsWithCmd = []string{"machine", "name", "port", "idc", "cmd"}
)
