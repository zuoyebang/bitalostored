#!/bin/bash

DEMO_ROOT_DIR=$(pwd)/demo
DEMO_BIN_DIR=$DEMO_ROOT_DIR/bin
DEMO_CONF_DIR=$DEMO_ROOT_DIR/conf
DEMO_LOG_DIR=$DEMO_ROOT_DIR/log
DEMO_STORED_LOG_DIR=$DEMO_LOG_DIR/bitalostored
DEMO_DASHBOARD_LOG_DIR=$DEMO_LOG_DIR/bitalosdashboard
DEMO_FE_LOG_DIR=$DEMO_LOG_DIR/bitalosfe
DEMO_PROXY_LOG_DIR=$DEMO_LOG_DIR/bitalosproxy
DEMO_COOKIES_FILE=$DEMO_ROOT_DIR/cookies.txt

INIT_SERVER_PORT=19091
INIT_RAFT_PORT=19081
PROXY_PORT=8790
DASHBOARD_PORT=18080
FE_PORT=8080
PRODUCT_NAME=bitalos-demo
AUTH=56391ed147981d58b6c72a60c010b9f0
USERNAME=demo
PASSWORD=demo


createSqlite() {
    DB_FILE=$1
    sqlite3 $DB_FILE <<EOF
CREATE TABLE IF NOT EXISTS tblDashboard (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name varchar(512) NOT NULL DEFAULT '',
    sub_path varchar(512) NOT NULL DEFAULT '',
    full_path varchar(512) NOT NULL DEFAULT '',
    value text,
    create_time int unsigned NOT NULL DEFAULT '0',
    update_time int unsigned NOT NULL DEFAULT '0'
);
EOF
    sqlite3 $DB_FILE "delete from tblDashboard;"
    loginValue="{\"username\":\"$USERNAME\",\"password\":\"$PASSWORD\",\"role\":1}"
    sqlite3 $DB_FILE "INSERT INTO tblDashboard (product_name, sub_path,full_path,value) VALUES ('admin', '$USERNAME','/stored/admin/$USERNAME','$loginValue');"
}

generateDashboardConf() {
    cat <<EOF > $DEMO_CONF_DIR/bitalosdashboard.toml
coordinator_name = "sqlite"
coordinator_addr = "bitalos-demo.db"

# Set Stored Product Name/Auth.
product_name = "$PRODUCT_NAME"
product_auth = ""

# Set bind address for admin(rpc), tcp only.
admin_addr = "0.0.0.0:$DASHBOARD_PORT"
# Set Stored raft
admin_model  = "raft"

[database]
username = "admin"
password = "admin"
hostport = "127.0.0.1:13306"
dbname = "bitalos-demo"
EOF
}

generateProxyConf() {
    cat <<EOF > $DEMO_CONF_DIR/bitalosproxy.toml
product_name = "$PRODUCT_NAME"
product_auth = ""

proxy_auth_enabled = false
proxy_auth_password = "bitalosproxy.clustername"
proxy_auth_admin = "bitalosproxy.clustername.admin"

dashboard_proto_type = "http"
dashboard_username = "$USERNAME"
dashboard_password = "$PASSWORD"

proto_type = "tcp4"
proxy_addr = "0.0.0.0:$PROXY_PORT"
admin_addr = "0.0.0.0:$((PROXY_PORT+1))"

proxy_cloudtype = "txcloud"
read_cross_cloud = 1
proxy_max_clients = 1000
max_procs = 4
conn_read_buffersize = "4kb"
conn_write_buffersize = "4kb"
conn_keepalive = "180s"

local_cache_expiretime = "2m"
local_cache_cleanuptime = "5m"
local_cache_bucketnum  = 16

pprof_switch = 0
pprof_address = ":8113"

metrics_report_log_switch = 1
metrics_report_log_period = "5s"
metrics_exporter_switch = 1
metrics_exporter_period = "5s"
metrics_reset_cycle = 2

open_distributed_tx = false

breaker_stop_timeout = "200ms"
breaker_open_fail_rate = 0.05
breaker_restore_request = 50

[log]
is_debug = false
rotation_time = "Hourly"
log_file = "$DEMO_PROXY_LOG_DIR/proxy.log"
stats_log_file = "$DEMO_PROXY_LOG_DIR/proxy-stats.log"
access_log = false
access_log_file = "$DEMO_PROXY_LOG_DIR/proxy.access.log"
slow_log = true
slow_log_cost = "30ms"
slow_log_file = "$DEMO_PROXY_LOG_DIR/proxy.slow.log"

[redis_default_conf]
max_idle = 100
max_active = 600
idle_timeout = "3600s"
conn_lifetime = "3600s"
password = ""
database = 0
conn_timeout = "500ms" 
read_timeout = "1s"
write_timeout = "1s"
total_connection = 40

[dynamic_deadline]
client_ratio_threshold = [0,30,60,80,90]
deadline_threshold = ["180s","100s","30s","6s","2s"]
EOF
}

#$1 server_port $2 node_id $3 cluster_id $4 raft_port $5 addresslist $6 nodelist $7 is_witness
generateStoredConf() {
    cat <<EOF > $DEMO_CONF_DIR/bitalostored-$3-$2.toml
[server]
product_name = "$PRODUCT_NAME"
address = ":$1"
max_client = 15000
keep_alive = "600s"
max_procs = 12
db_path = "$DEMO_ROOT_DIR/bitalostored-$3-$2"
slow_time = "30ms"
slow_key_window_time = "2000ms"
slow_shield = true
slow_ttl  = "1s"
slow_maxexec = 100
slow_topn = 100
token = "token"
degrade_signle_node = false
open_distributed_tx = false

[plugin]
open_raft = true
open_panic = true
open_pprof = false
pprof_addr = ":26770"
open_gops = false

[log]
is_debug = false
rotation_time = "Daily"

[bitalos]
write_buffer_size = "256mb" # default
enable_wal = true
compact_start_time = 1
compact_end_time = 6
compact_interval = 300        
bithash_gc_threshold = 0.5
bithash_compression_type = 0            
enable_expired_deletion = true       
expired_deletion_interval = 60 
expired_deletion_qps_threshold = 20000 # default
io_write_qps_threshold = 20000 # default
max_field_size = 10240 # default
max_value_size = 6291456 # default
enable_raftlog_restore = false # default
enable_page_block_compression = false # default
enable_clock_cache = false # default
cache_size = 0 # default

[raft_queue]
workers = 60             
length = 10000           

[raft_cluster]
cluster_id = $3
election_rtt = 35
preelection_rtt = 10
heartbeat_rtt = 1
check_quorm = true
snapshot_entries = 6000000
compaction_overhead = 500000
snapshot_compression_type = 1
entry_compression_type = 1
disable_auto_compactions = false
timeout = "2s"
retry_times = 1
async_propose = true
is_witness = $7

[raft_nodehost]
node_id = $2
raft_address = "127.0.0.1:$4"
init_raft_addrlist = [$5]
init_raft_nodelist = [$6]
join = false
snapshot_timeout = "10s"
rtt = 200
deployment_id = 0
max_snapshot_send_bytes_persecod = "60mb"
max_snapshot_recv_bytes_persecod = "60mb"

[raft_state]
interval = "3s"
allow_max_offset = 100000

[dynamic_deadline]
client_ratio_threshold = [0,20,50,80,90]
deadline_threshold = ["1800s","600s","180s","60s","10s"]
EOF
}

checkSqlite() {
    if which sqlite3 >/dev/null 2>&1; then
        echo "check SQLite is installed"
    else
        echo "check SQLite is not installed"
        exit 0
    fi
}

deployDh() {
    #checkSqlite
    #createSqlite $DEMO_ROOT_DIR/$PRODUCT_NAME.db
    generateDashboardConf
    nohup bin/bitalosfe --assets-dir=bin/dist "--sqlite=$DEMO_ROOT_DIR/${PRODUCT_NAME}.db" --log=$DEMO_FE_LOG_DIR/fe.log --pidfile=$DEMO_ROOT_DIR/bitalosfe.pid --log-level=INFO --listen=0.0.0.0:$FE_PORT >> $DEMO_FE_LOG_DIR/fe.out 2>&1 &
    nohup bin/bitalosdashboard --config=$DEMO_CONF_DIR/bitalosdashboard.toml  "--sqlite=$DEMO_ROOT_DIR/${PRODUCT_NAME}.db" --log=$DEMO_DASHBOARD_LOG_DIR/dashboard.log --log-level=INFO --pidfile=$DEMO_ROOT_DIR/bitalosdashboard.pid >> $DEMO_DASHBOARD_LOG_DIR/dashboard.out 2>&1 &
    sleep 5
    getLoginCookies
}

#$1 slaveNum $2 witnessNum $3 groupNum
deployServer() {
    serverPort=$INIT_SERVER_PORT
    raftPort=$INIT_RAFT_PORT
    for ((g=1; g<=$3; g++))
    do
        raftPortInit=$raftPort
        res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/group/create/$AUTH/$g)
        echo "Create group $g. Response $res"
        raftAddress=""
        nodeList=""
        for ((s=1; s<=$1+1; s++))
        do
            raftAddress+="\"127.0.0.1:$raftPort\","
            raftPort=$((raftPort+1))
            nodeList+="$s,"
        done
        raftAddress=${raftAddress%,}
        nodeList=${nodeList%,}

        for ((s=1; s<=$1+1; s++))
        do
            generateStoredConf $serverPort $s $g $raftPortInit $raftAddress $nodeList false
            cp bin/bitalostored $DEMO_BIN_DIR/bitalostored-$g-$s
            echo "Start group $g normal node 127.0.0.1:$serverPort"
            nohup $DEMO_BIN_DIR/bitalostored-$g-$s --conf.file=$DEMO_CONF_DIR/bitalostored-$g-$s.toml >> $DEMO_STORED_LOG_DIR/stored-$g-$s.out 2>&1 & echo $! > $DEMO_ROOT_DIR/bitalostored-$g-$s.pid
            sleep 10
            res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/group/add/$AUTH/$g/127.0.0.1:$serverPort/txcloud/master_slave_node)
            echo "Add normal node 127.0.0.1:$serverPort to group $g. Response $res"
            serverPort=$((serverPort+1))
            raftPortInit=$((raftPortInit+1))
        done

        for ((w=$s; w<$2+$s; w++))
        do
            generateStoredConf $serverPort $w $g $raftPortInit $raftAddress $nodeList true
            cp bin/bitalostored $DEMO_BIN_DIR/bitalostored-$g-$w
            echo "Start group $g witness node 127.0.0.1:$serverPort"
            nohup $DEMO_BIN_DIR/bitalostored-$g-$w --conf.file=$DEMO_CONF_DIR/bitalostored-$g-$w.toml >> $DEMO_STORED_LOG_DIR/stored-$g-$w.out 2>&1 & echo $! > $DEMO_ROOT_DIR/bitalostored-$g-$w.pid
            sleep 10
            res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/group/add/$AUTH/$g/127.0.0.1:$serverPort/txcloud/witness_node)
            echo "Add witness node 127.0.0.1:$serverPort to group $g. Response $res"
            sleep 2
            res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/group/mount/$AUTH/$g/127.0.0.1:$serverPort/127.0.0.1:$raftPortInit/$w/4)
            echo "Mount witness node 127.0.0.1:$serverPort to group $g. Response $res"
            serverPort=$((serverPort+1))
            raftPortInit=$((raftPortInit+1))
            raftPort=$((raftPort+1))
        done
    done
}

deployProxy() {
    echo "Proxy Deploying"
    generateProxyConf
    nohup bin/bitalosproxy --config=$DEMO_CONF_DIR/bitalosproxy.toml --dashboard=127.0.0.1:$FE_PORT --pidfile=$DEMO_ROOT_DIR/bitalosproxy.pid >> $DEMO_PROXY_LOG_DIR/proxy.out 2>&1 &
    sleep 1
}

initDir() {
    if [ -d "$DEMO_ROOT_DIR" ]; then
        echo "clear dir: demo"
        rm -r $DEMO_ROOT_DIR
    fi
    echo "mkdir demo"
    mkdir -p $DEMO_BIN_DIR $DEMO_CONF_DIR $DEMO_DASHBOARD_LOG_DIR $DEMO_FE_LOG_DIR $DEMO_PROXY_LOG_DIR $DEMO_STORED_LOG_DIR
}

echoInfo() {
    echo "Build Demo Successfully!"
    echo "Dashboard Address: 127.0.0.1:$FE_PORT/#/$PRODUCT_NAME"
    echo "Dashboard Username: $USERNAME"
    echo "Dashboard Paasword: $PASSWORD"
    echo "Proxy Address: 127.0.0.1:$PROXY_PORT"
}

getLoginCookies() {
    res=$(curl -c $DEMO_COOKIES_FILE -s -d "username=$USERNAME&password=$PASSWORD" http://127.0.0.1:$FE_PORT/login)
    echo "Get login cookies. Response $res"
}

checkPort() {
    #fe
    if lsof -i :$FE_PORT |grep LISTEN >/dev/null; then
        echo "FE Port $FE_PORT is listening."
        exit 0
    else
        echo "Check FE Port $FE_PORT is ok."
    fi
    #dashboard
    if lsof -i :$DASHBOARD_PORT |grep LISTEN >/dev/null; then
        echo "Dashboard Port $DASHBOARD_PORT is listening."
        exit 0
    else
        echo "Check Dashboard Port $DASHBOARD_PORT is ok."
    fi
    #proxy
    if lsof -i :$PROXY_PORT |grep LISTEN >/dev/null; then
        echo "Proxy Port $PROXY_PORT is listening."
        exit 0
    else
        echo "Check Proxy Port $PROXY_PORT is ok."
    fi
    proxyAdminPort=$((PROXY_PORT+1))
    if lsof -i :$proxyAdminPort |grep LISTEN >/dev/null; then
        echo "Proxy Admin Port $proxyAdminPort is listening."
        exit 0
    else
        echo "Check Proxy Admin Port $proxyAdminPort is ok."
    fi
    #stored
    for ((i=$INIT_SERVER_PORT; i<$INIT_SERVER_PORT+$1; i++))
    do
        if lsof -i :$i |grep LISTEN >/dev/null; then
            echo "Stored Port $i is listening."
            exit 0
        else
            echo "Check Stored Port $i is ok."
        fi
    done
    for ((i=$INIT_RAFT_PORT; i<$INIT_RAFT_PORT+$1; i++))
    do
        if lsof -i :$i |grep LISTEN >/dev/null; then
            echo "Stored Raft Port $i is listening."
            exit 0
        else
            echo "Check Stored Raft Port $i is ok."
        fi
    done
}

main() {
    read -p "Slave nums:(default 1)" slaveNum
    if [ -z "$slaveNum" ]; then
        slaveNum=1
    elif [[ ! $slaveNum =~ ^[0-9]+$ ]]; then
        echo "invalid input, use default 1"
        slaveNum=1
    fi
    read -p "Witness nums:(default 1)" witnessNum
    if [ -z "$witnessNum" ]; then
        witnessNum=1
    elif [[ ! $witnessNum =~ ^[0-9]+$ ]]; then
        echo "invalid input, use default 1"
        witnessNum=1
    fi
    read -p "Group nums:(default 2)" groupNum
    if [ -z "$groupNum" ]; then
        groupNum=2
    elif [[ ! $groupNum =~ ^[0-9]+$ || $groupNum -lt 1 ]]; then
        echo "invalid input, use default 2"
        groupNum=2
    fi
    nodeNum=$(( ($slaveNum + 1 + $witnessNum) * $groupNum ))
    checkPort $nodeNum
    initDir
    echo "Start build bitalos"
    make
    echo "Start deploy dashboard"
    deployDh
    echo "Start depoly stored"
    deployServer $slaveNum $witnessNum $groupNum
    sleep 5
    deployProxy
    sleep 15
    res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/group/replica-groups-all/$AUTH/1)
    echo "Replica all groups. Response $res"
    sleep 5
    res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/group/resync-all/$AUTH)
    echo "Resync all groups. Response $res"
    sleep 5
    res=$(curl -b $DEMO_COOKIES_FILE -s -X PUT 127.0.0.1:$DASHBOARD_PORT/api/topom/slots/action/create/init/$AUTH)
    echo "Slots init. response $res"
    sleep 10
    echoInfo
}

main $@
