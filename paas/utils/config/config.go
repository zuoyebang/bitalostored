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

package config

import (
	"bytes"
	"github.com/zuoyebang/bitalostored/paas/utils/bytesize"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"text/template"
)

type PaaSConf struct {
	PaasServer  PaaSServer  `toml:"paas_server" json:"paasServer"`
	Cos         COS         `toml:"cos" json:"cos"`
	Robot       Robot       `toml:"robot" json:"robot"`
	Deploy      Deploy      `toml:"deploy" json:"deploy"`
	Database    Database    `toml:"database" json:"database"`
	RedisAuths  []RedisAuth `toml:"redis_auths" json:"redisAuths"`
	BuildServer BuildServer `toml:"build_server" json:"buildServer"`
	FE          FE          `toml:"fe"`
	Domains     Domain      `toml:"domain"`
}

type Robot struct {
	OpDing   string `toml:"op_ding"`
	RaftDing string `toml:"raft_ding"`
}

type COS struct {
	Supervisor string `toml:"supervisor"`
}

type Deploy struct {
	DeployPath string `toml:"deploy_path"`
}
type Domain struct {
	DashboardDomain string `toml:"dashboard_domain"`
	Dashboard       string `toml:"dashboard"`
}

type FE struct {
	Assets string `toml:"assets" json:"assets"`
}

type BuildServer struct {
	Address  string `toml:"address" json:"address"`
	LocalDir string `toml:"local_dir" json:"local_dir"`
}

type PaaSServer struct {
	ListenPort int    `toml:"listen_port" json:"listenPort"`
	PaaSEnv    string `toml:"paas_env" json:"paasEnv"`
	PaasArea   string `toml:"paas_area" json:"paasArea"`
	DhUsername string `toml:"dh_username" json:"dhUsername"`
	DhPassword string `toml:"dh_password" json:"dhPassword"`
	GrafanaUrl string `toml:"grafana_url" json:"grafanaUrl"`
}

func (c *PaaSConf) Validate() error {
	if c.PaasServer.ListenPort <= 0 || c.PaasServer.ListenPort >= 65536 {
		return errors.New("invalid port config")
	}
	return nil
}

type Database struct {
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
	Hostport string `toml:"hostport" json:"hostport"`
	Database string `toml:"database" json:"database"`
	Download string `toml:"download" json:"download"`
	FilePath string `toml:"filepath" json:"filepath"`
}

type RedisAuth struct {
	AdminAuth   string `toml:"admin_auth" json:"adminAuth"`
	ClusterId   int    `toml:"cluster_id" json:"clusterId"`
	ClusterName string `toml:"cluster_name" json:"clusterName"`
}

type SConfig struct {
	Log                ServerLogConfig         `toml:"log" mapstructure:"log"`
	Plugin             PluginConfig            `toml:"plugin" mapstructure:"plugin"`
	Server             ServerConfig            `toml:"server" mapstructure:"server"`
	Bitalos            BitalosConfig           `toml:"bitalos" mapstructure:"bitalos"`
	RaftQueue          RaftQueueConfig         `toml:"raft_queue" mapstructure:"raft_queue"`
	RaftCluster        RaftClusterConfig       `toml:"raft_cluster" mapstructure:"raft_cluster"`
	RaftNodeHost       RaftNodeHostConfig      `toml:"raft_nodehost" mapstructure:"raft_nodehost"`
	RaftState          RaftStateConfig         `toml:"raft_state" mapstructure:"raft_state"`
	FiexedRaftNodeHost FixedRaftNodeHostConfig `toml:"-" mapstructure:"-"`
	FiexedRaftCluster  FixedRaftClusterConfig  `toml:"-" mapstructure:"-"`
	DynamicDeadline    DynamicDeadline         `toml:"dynamic_deadline" mapstructure:"dynamic_deadline"`
}

type ServerLogConfig struct {
	IsDebug      bool   `toml:"is_debug" mapstructure:"is_debug"`
	RotationTime string `toml:"rotation_time" mapstructure:"rotation_time"`
}

type ServerConfig struct {
	ProductName          string         `toml:"product_name" mapstructure:"product_name"`
	Address              string         `toml:"address" mapstructure:"address"`
	Maxclient            int64          `toml:"max_client" mapstructure:"max_client"`
	Keepalive            math2.Duration `toml:"keep_alive" mapstructure:"keep_alive"`
	Maxprocs             int            `toml:"max_procs" mapstructure:"max_procs"`
	ConfigFile           string         `toml:"config_file" mapstructure:"config_file"`
	DBPath               string         `toml:"db_path" mapstructure:"db_path"`
	DisableEdgeTriggered bool           `toml:"disable_edge_triggered" mapstructure:"disable_edge_triggered"`
	NetEventLoopNum      int            `toml:"net_event_loop_num" mapstructure:"net_event_loop_num"`
	NetWriteBuffer       bytesize.Int64 `toml:"net_write_buffer" mapstructure:"net_write_buffer"`

	SlowShield        bool           `toml:"slow_shield" mapstructure:"slow_shield"`
	SlowTime          math2.Duration `toml:"slow_time" mapstructure:"slow_time"`
	SlowKeyWindowTime math2.Duration `toml:"slow_key_window_time" mapstructure:"slow_key_window_time"`
	SlowTTL           math2.Duration `toml:"slow_ttl" mapstructure:"slow_ttl"`
	SlowMaxExec       int            `toml:"slow_maxexec" mapstructure:"slow_maxexec"`
	SlowTopN          int            `toml:"slow_topn" mapstructure:"slow_topn"`

	Token             string `toml:"token" mapstructure:"token"`
	DegradeSingleNode bool   `toml:"degrade_signle_node" mapstructure:"degrade_signle_node"`
	OpenDistributedTx bool   `toml:"open_distributed_tx" mapstructure:"open_distributed_tx"`
}

type BitalosConfig struct {
	WriteBufferSize                bytesize.Int64 `toml:"write_buffer_size" mapstructure:"write_buffer_size"`
	CacheSize                      bytesize.Int64 `toml:"cache_size" mapstructure:"cache_size"`
	CacheHashSize                  int            `toml:"cache_hash_size" mapstructure:"cache_hash_size"`
	CacheShardNum                  int            `toml:"cache_shard_num" mapstructure:"cache_shard_num"`
	CacheEliminateDuration         int            `toml:"cache_eliminate_duration" mapstructure:"cache_eliminate_duration"`
	EnableMissCache                bool           `toml:"enable_miss_cache" mapstructure:"enable_miss_cache"`
	CompactStartTime               int            `toml:"compact_start_time" mapstructure:"compact_start_time"`
	CompactEndTime                 int            `toml:"compact_end_time" mapstructure:"compact_end_time"`
	CompactInterval                int            `toml:"compact_interval" mapstructure:"compact_interval"`
	BithashGcThreshold             float64        `toml:"bithash_gc_threshold" mapstructure:"bithash_gc_threshold"`
	BithashCompressionType         int            `toml:"bithash_compression_type" mapstructure:"bithash_compression_type"`
	CompressionType                int            `toml:"compression_type" mapstructure:"compression_type"`
	IOWriteLoadQpsThreshold        uint64         `toml:"io_write_qps_threshold" mapstructure:"io_write_qps_threshold"`
	MaxFieldSize                   int            `toml:"max_field_size" mapstructure:"max_field_size"`
	MaxValueSize                   int            `toml:"max_value_size" mapstructure:"max_value_size"`
	EnablePageBlockCompression     bool           `toml:"enable_page_block_compression" mapstructure:"enable_page_block_compression"`
	PageBlockCacheSize             bytesize.Int64 `toml:"page_block_cache_size" mapstructure:"page_block_cache_size"`
	EnableClockCache               bool           `toml:"enable_clock_cache" mapstructure:"enable_clock_cache"`
	FlushPrefixDeleteKeyMultiplier int            `toml:"flush_prefix_delete_key_multiplier" mapstructure:"flush_prefixdeletekey_multiplier"`
	FlushFileLifetime              int            `toml:"flush_file_lifetime" mapstructure:"flush_file_lifetime"`
	BitmapCacheItemCount           int            `toml:"bitmap_cache_item_count" mapstructure:"bitmap_cache_item_count"`
	BitpageFlushSize               bytesize.Int64 `toml:"bitpage_flush_size" mapstructure:"bitpage_flush_size"`
	BitpageSplitSize               bytesize.Int64 `toml:"bitpage_split_size" mapstructure:"bitpage_split_size"`
	BitpageDisableMiniVi           bool           `toml:"bitpage_disable_minivi" mapstructure:"bitpage_disable_minivi"`
	DisableStoreKey                bool           `toml:"disable_store_key" mapstructure:"disable_store_key"`
	VectorTableCount               int            `toml:"vector_table_count" mapstructure:"vector_table_count"`
	VectorTableHashSize            int            `toml:"vector_table_hash_size" mapstructure:"vector_table_hash_size"`
	VectorTableGcThreshold         float64        `toml:"vector_table_gc_threshold" mapstructure:"vector_table_gc_threshold"`
	MemTableSize                   bytesize.Int64 `toml:"mem_table_size" mapstructure:"mem_table_size"`
	VmTableSize                    bytesize.Int64 `toml:"vm_table_size" mapstructure:"vm_table_size"`
}

type RaftQueueConfig struct {
	Workers int `toml:"workers" mapstructure:"workers"`
	Length  int `toml:"length" mapstructure:"length"`
}

type RaftNodeHostConfig struct {
	NodeID                        uint64         `toml:"node_id" mapstructure:"node_id"`
	HostName                      string         `toml:"host_name" mapstructure:"host_name"`
	RaftAddress                   string         `toml:"raft_address" mapstructure:"raft_address"`
	InitRaftAddrList              []string       `toml:"init_raft_addrlist" mapstructure:"init_raft_addrlist"`
	InitRaftNodeList              []uint64       `toml:"init_raft_nodelist" mapstructure:"init_raft_nodelist"`
	SnapshotTimeout               math2.Duration `toml:"snapshot_timeout" mapstructure:"snapshot_timeout"`
	Rtt                           uint64         `toml:"rtt" mapstructure:"rtt"`
	DeploymentId                  uint64         `toml:"deployment_id" mapstructure:"deployment_id"`
	MaxSnapshotSendBytesPerSecond bytesize.Int64 `toml:"max_snapshot_send_bytes_persecod" mapstructure:"max_snapshot_send_bytes_persecod"`
	MaxSnapshotRecvBytesPerSecond bytesize.Int64 `toml:"max_snapshot_recv_bytes_persecod" mapstructure:"max_snapshot_recv_bytes_persecod"`
}

type RaftStateConfig struct {
	Interval       math2.Duration `toml:"interval" mapstructure:"interval"`
	AllowMaxOffset int64          `toml:"allow_max_offset" mapstructure:"allow_max_offset"`
}

type RaftClusterConfig struct {
	ClusterId               uint64         `toml:"cluster_id" mapstructure:"cluster_id"`
	ElectionRTT             uint64         `toml:"election_rtt" mapstructure:"election_rtt"`
	HeartbeatRTT            uint64         `toml:"heartbeat_rtt" mapstructure:"heartbeat_rtt"`
	CheckQuorum             bool           `toml:"check_quorm" mapstructure:"check_quorm"`
	SnapshotEntries         uint64         `toml:"snapshot_entries" mapstructure:"snapshot_entries"`
	CompactionOverhead      uint64         `toml:"compaction_overhead" mapstructure:"compaction_overhead"`
	SnapshotCompressionType int32          `toml:"snapshot_compression_type" mapstructure:"snapshot_compression_type"`
	EntryCompressionType    int32          `toml:"entry_compression_type" mapstructure:"entry_compression_type"`
	DisableAutoCompactions  bool           `toml:"disable_auto_compactions" mapstructure:"disable_auto_compactions"`
	TimeOut                 math2.Duration `toml:"timeout" mapstructure:"timeout"`
	RetryTimes              int            `toml:"retry_times" mapstructure:"retry_times"`
	AsyncPropose            bool           `toml:"async_propose" mapstructure:"async_propose"`
	IsObserver              bool           `toml:"is_observer" mapstructure:"is_observer"`
	IsWitness               bool           `toml:"is_witness" mapstructure:"is_witness"`
	Join                    bool           `toml:"join" mapstructure:"join"`
}

type FixedRaftNodeHostConfig struct {
	HostName    string `json:"host_name"`
	RaftAddress string `json:"raft_address"`

	Rtt                           uint64         `json:"rtt"`
	DeploymentId                  uint64         `json:"deployment_id"`
	MaxSendQueueSize              uint64         `json:"max_send_queue_size"`
	MaxReceiveQueueSize           uint64         `json:"max_receive_queue_size"`
	MaxSnapshotSendBytesPerSecond bytesize.Int64 `json:"max_snapshot_send_bytes_persecod"`
	MaxSnapshotRecvBytesPerSecond bytesize.Int64 `json:"max_snapshot_recv_bytes_persecod"`
}

type FixedRaftClusterConfig struct {
	ElectionRTT             uint64         `json:"election_rtt"`
	HeartbeatRTT            uint64         `json:"heartbeat_rtt"`
	CheckQuorum             bool           `json:"check_quorm"`
	SnapshotEntries         uint64         `json:"snapshot_entries"`
	CompactionOverhead      uint64         `json:"compaction_overhead"`
	MaxInMemLogSize         uint64         `json:"max_in_mem_logsize"`
	SnapshotCompressionType int32          `json:"snapshot_compression_type"`
	EntryCompressionType    int32          `json:"entry_compression_type"`
	DisableAutoCompactions  bool           `json:"disable_auto_compactions"`
	SnapshotTimeout         math2.Duration `json:"snapshot_timeout"`
	TimeOut                 math2.Duration `json:"timeout"`
	RetryTimes              int            `json:"retry_times"`
	AsyncPropose            bool           `json:"async_propose"`
}

type PluginConfig struct {
	OpenRaft  bool   `toml:"open_raft" mapstructure:"open_raft"`
	OpenPprof bool   `toml:"open_pprof" mapstructure:"open_pprof"`
	PprofAddr string `toml:"pprof_addr" mapstructure:"pprof_addr"`
}

type DynamicDeadline struct {
	ClientRatios      []int            `toml:"client_ratio_threshold" json:"client_ratio_threshold"`
	DeadlineThreshold []math2.Duration `toml:"deadline_threshold" json:"deadline_threshold"`
}

func Render(source string, data interface{}) (string, error) {
	tmpl, err := template.New("").Parse(source)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
