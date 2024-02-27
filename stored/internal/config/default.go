// Copyright 2019 The Bitalostored author and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

const DefaultConfig = `
[server] 
address = ":10091"      
max_client = 5000      
keep_alive = "3600s"   
max_procs = 8         
db_path = "bitalosdb" 
slow_time = "40ms"   
slow_key_window_time = "2000ms" 
slow_shield = true   
slow_ttl  = "1s"  
slow_maxexec = 100 
slow_topn = 100  
token = "token" 
degrade_signle_node = false

[plugin]
open_raft = true
open_panic = true
open_pprof = false
pprof_addr = ":26770"
open_gops = false

[log]
is_debug = true
rotation_time = "Daily"

[bitalos]
write_buffer_size = "256mb"

[worker_queue]
enable = true       
timeout = "2s"      
kv_worker = 30      
list_worker = 30     
hash_work = 30      
set_worker = 30    
zset_worker = 30

[raft_queue]
workers = 20            
length = 1000000       

[raft_cluster]
cluster_id = 1                          
election_rtt = 10                        
heartbeat_rtt = 1                        
check_quorm = true                     
snapshot_entries = 5000000                
compaction_overhead = 1000000                
snapshot_compression_type = 0            
entry_compression_type = 0              
disable_auto_compactions = false          
timeout = "2s"                              
retry_times = 1                          
async_propose = false                   

[raft_nodehost]
node_id = 1                            
raft_address = ":61001"               
init_raft_addrlist = ["localhost:63001","localhost:63002","localhost:63003"] 
join = false                         
snapshot_timeout = "10s"            
rtt = 200                           
deployment_id = 0                     
max_snapshot_send_bytes_persecod = "20mb"
MaxSnapshotRecvBytesPerSecond = "20mb" 

[raft_state]
interval = "1s"                         
allow_max_offset = 7500   

[dynamic_deadline]
client_ratio_threshold = [0,20,50,80,90]
deadline_threshold = ["1800s","600s","180s","60s","10s"]
`
