package task

type TaskInfo struct {
	TaskId     int64  `json:"taskId"`
	TaskType   string `json:"taskType"`
	TaskStatus string `json:"taskStatus"`

	MachineId int `json:"machineId"`
	ServiceId int `json:"serviceId"`
	ClusterId int `json:"clusterId"`
	GroupId   int `json:"groupId"`
	NodeId    int `json:"nodeId"`

	TaskExt struct {
		RegionName       string `json:"regionName"`
		ServiceName      string `json:"serviceName"`
		ServicePort      int    `json:"servicePort"`
		ServicePortRange []int  `json:"servicePortRange"`
		ClusterPortRange []int  `json:"clusterPortRange"`
		ClusterName      string `json:"clusterName"`
		ClusterPort      int    `json:"clusterPort"`
		DashboardAddress string `json:"dashboardAddress"`
		CloudType        string `json:"cloudType"`
		StoredAuth       string `json:"storedAuth"`
		Operation        string `json:"operation"`

		Ip          string   `json:"ip"`
		NodeIndex   uint     `json:"nodeIndex"`
		NodeList    []string `json:"nodeList"`
		NodeListStr string
		NodeListVal string
		IsObserver  bool `json:"isObserver"`
		IsWitness   bool `json:"isWitness"`
		Snapshot    uint `json:"snapshot"`
		IsJoin      bool
		//ConfigPath  string

		ExtString string `json:"extString"`

		TargetGroupId uint `json:"targetGroupId"`

		PaasExt interface{}
	} `json:"taskExt"`

	TaskRoot string
	TaskPath string

	TemplateFiles []string

	TaskFiles []TaskFile `json:"taskFiles"`
}

type TaskFile struct {
	FileType string `json:"fileType"`
	FileMode string `json:"fileMode"`
	FileName string `json:"fileName"`
	CosKey   string `json:"cosKey"`
	Content  string `json:"content"`
}
