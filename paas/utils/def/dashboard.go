// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package def

const (
	AlertInfoMasterSwitch        = "switch-master manual"
	AlertInfoMasterSwitchTimeout = "switch-master timeout"
	AlertInfoIOTimeoutProxy      = "i/o timeout same proxy"
	AlertInfoIOTimeoutServer     = "i/o timeout same server"
	AlertInfoIOTimeoutKey        = "i/o timeout same key"
	AlertInfoNetTimeout          = "network timeout"
	AlertInfoConnectRefused      = "connection refused"
	AlertInfoRaft                = "raft connect failed"
	AlertInfoPanicProxy          = "proxy panic"
	AlertInfoPanicServer         = "server panic"
	AlertInfoProxySameSLowKey    = "proxy most of the slow keys are the same"
	AlertInfoServerSameSLowKey   = "server most of the slow keys are the same"
	AlertInfoAddressSlow         = "single address more slow"

	AlertInfoProxySlowCntMore       = "proxy slow cnt too much"
	AlertInfoProxySlowCostLarge     = "proxy slow cost too large"
	AlertInfoServerSlowCntMore      = "server slow cnt too much"
	AlertInfoServerSlowCostLarge    = "server slow cost too large"
	AlertInfoProxyAvgCostPrev       = "proxy avg cost prev"
	AlertInfoProxyAvgCostYesterday  = "proxy avg cost yesterday"
	AlertInfoProxyAvgCostWeek       = "proxy avg cost last week"
	AlertInfoServerRealCpuPrev      = "server cpu qps prev"
	AlertInfoServerRealCpuYesterday = "server cpu qps yesterday"
	AlertInfoServerRealCpuWeek      = "server cpu qps last week"
	AlertInfoServerMaxCpu           = "server cpu almost reach limit"
	AlertInfoProxyMaxCpu            = "proxy cpu almost reach limit"
	AlertInfoServerCpuThrottled     = "server cpu throttled increase"
	AlertInfoProxyCpuThrottled      = "proxy cpu throttled increase"

	AlertInfoWrongMaster      = "wrong master cloud"
	AlertInfoRaftFalse        = "raft status false"
	AlertInfoServerDown       = "server down"
	AlertInfoRepeatMachine    = "group has repeat machine"
	AlertInfoServerNotReplica = "server not replica"
	AlertInfoDeraftGroup      = "deraft group"
	AlertInfoOutOfSync        = "out of sync"
	AlertInfoGroupWrongNode   = "group has wrong node"
	AlertInfoProxyDown        = "proxy down"
)
