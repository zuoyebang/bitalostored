#!/bin/bash

SERVICE_HOME=/home/homework/prometheus/prometheus-2.14.0
SERVICE_BIN_HOME=${SERVICE_HOME}
SERVICE_NAME=prometheus 
SERVICE_BIN=${SERVICE_BIN_HOME}/${SERVICE_NAME}
PIDFILE=${SERVICE_BIN}.pid

RETVAL=0

start() {
    echo -n $"Starting SERVICE_BIN: "
    # 校验pid文件
    if [ -f $PIDFILE ]
    then
        PID=`cat ${PIDFILE}`
        RunningPID=`ps aux|grep ${SERVICE_BIN}|grep -v grep| awk '{print $2}'`
        # 判断是否有目标进程启动
        if [ "$PID" != "$RunningPID" ]
        then
            rm $PIDFILE
        else
            echo "http server already start, PID:["${PID}"]"
            return
        fi
    fi
    # 确保可执行
    if [ -x $SERVICE_BIN ]
    then
        echo "$SERVICE_BIN 可执行"
    else
        echo "$SERVICE_BIN 不可执行，chmod a+x"
        chmod a+x $SERVICE_BIN
    fi
    # 命令
    export GIN_MODE=release
    nohup $SERVICE_BIN --storage.tsdb.wal-segment-size=256MB --storage.tsdb.retention.time=14d --storage.tsdb.retention.size=200GB 2>>$SERVICE_BIN_HOME/${SERVICE_NAME}.dump &
    # 启动结果
    sleep 3
    status
    rc=$?
    if [[ $rc != 0 ]]
    then
        echo "$(date) Failed to start $SERVICE_NAME, return code: $rc"
        exit $rc;
    fi
    # 写pid文件
    echo $! > $PIDFILE
}

start_front() {
    $SERVICE_BIN
}

stop() {
    echo -n $"Stopping SERVICE_BIN: "
    # 命令
    PID=`ps aux|grep ${SERVICE_BIN}|grep -v grep| awk '{print $2}'`
    if [ -z "$PID" ]
    then
        echo "$SERVICE_NAME is already stopped"
    else
        kill $PID
    fi
    if [ -f $PIDFILE ]
    then
        rm $PIDFILE
    fi
    sleep 3
    status
}

restart() {
    stop
    sleep 1
    start
}

status() {
    pid=`ps -ef | grep "${SERVICE_BIN}" | grep -v grep | grep -v load_SERVICE_BIN | awk '{print $2}'`
    if [[ -z ${pid} ]]
    then
        echo "${SERVICE_NAME} status is: stopped"
        return 1
    else
        echo "${SERVICE_NAME} status is: running, pid is ${pid}"
        return 0
    fi
}


case "$1" in
start)
    if [ "x$2" == "xf" ] || [ "x$2" == "xfront" ]
    then
        start_front
    else
        start
    fi
    ;;

stop)
    stop
    ;;

restart)
    restart
    ;;

status)
    status
    ;;

*)

echo "Usage: $0 {start|stop|restart|status}"
echo $SERVICE_BIN
exit 1
esac
