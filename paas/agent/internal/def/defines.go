package def

// cgroup
const (
	CGROUP_CPU_PATH                = "/sys/fs/cgroup/cpu/stored"
	CGROUOP_CPU_SET_PATH           = "/sys/fs/cgroup/cpuset/stored"
	CGROUP_CPU_SHARE_PATH          = "/sys/fs/cgroup/cpuset/stored/stored-share"
	CGROUP_TASK_CPU                = "cpu"
	CGROUP_TASK_CPU_SET            = "cpu_set"
	CGROUP_TASK_CPU_SHARE          = "share_cpu_set"
	CGROUP_DEFAULT_PERIOD_US       = 50000
	CGROUP_DEFAULT_PROXY_QUOTA_US  = 200000
	CGROUP_DEFAULT_SERVER_QUOTA_US = 250000
)

const (
	URL_REGISTER = "machine/register"
	URL_LIST     = "task/list"
	URL_RECOVERY = "machine/recovery"
	URL_UPGRADED = "task/upgraded"
	URL_STATUS   = "task/status"
	URL_HOSTPORT = "task/hostport"
	URL_PREPARED = "task/prepared"
	URL_MANAGE   = "agent/manageinfo"
	URL_UPDATED  = "agent/updated"
	URL_DEPLOY   = "machine/nodedeployinfo"
)

const (
	TYPE_PREPARE_START = "prepareStart"
	TYPE_PREPARE_ADD   = "prepareAdd"
	TYPE_START         = "start"
	TYPE_ADD           = "add"
	TYPE_UPGRADE       = "upgrade"
	TYPE_OPERATE       = "operate"
	TYPE_CGROUP        = "cgroup"
	TYPE_LINK          = "link"

	TASK_OPERATION_APPLY_CGROUP = "apply_cgroup"
	TASK_OPERATION_RELEASE_CPUS = "release_cpus"
	TASK_OPERATION_SHARE_CPUS   = "apply_cgroup_share_cpus"
)

const (
	SERVICE_MATRIX  = "stored-matrix"
	SERVICE_BITALOS = "stored-bitalos"
	SERVICE_PROXY   = "stored-proxy"
)

const (
	LOG_SERVICE_SERVER = "server"
	LOG_SERVICE_PROXY  = "proxy"
)

const (
	SERVICE_TYPE_MATRIX    = 1
	SERVICE_TYPE_PROXY     = 2
	SERVICE_TYPE_DASHBOARD = 3
	SERVICE_TYPE_FE        = 4
	SERVICE_TYPE_AGENT     = 5
	SERVICE_TYPE_BITALOS   = 6
)

const (
	STATUS_ONLINE  = "online"
	STATUS_OFFLINE = "offline"
)

const (
	CPUSET_EXCLUSIVE = 1
	CPUSET_SHARE     = 0

	CGROUP_SERVER_NAME = "server"
	CGROUP_PROXY_NAME  = "proxy"
)
