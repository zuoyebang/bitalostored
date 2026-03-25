#!/bin/bash

SERVICE_HOME=/home/homework/prometheus/machine-exporter
SERVICE_CONF=${SERVICE_HOME}/conf/machine-exporter.toml
SERVICE_BIN_HOME=${SERVICE_HOME}/bin
SERVICE_NAME=machine_exporter
SERVICE_BIN=${SERVICE_BIN_HOME}/${SERVICE_NAME}
PIDFILE=${SERVICE_BIN}.pid
DUMPFILE=$SERVICE_BIN.dump

SUPERVISORD_BIN=${SERVICE_BIN_HOME}/supervisord
SUPERVISORD_CONF=${SERVICE_BIN_HOME}/supervisor.conf
SUPERVISORD_PIDFILE=${SERVICE_BIN_HOME}/supervisord.pid

supervisor-start() {
    echo -n $"check supervisor is running"
    # 校验pid文件
    if [ -f $SUPERVISORD_PIDFILE ]
    then
        PID=`cat ${SUPERVISORD_PIDFILE}`
        RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
        # 判断是否有目标进程启动
        if [ "$PID" != "" ]
        then
            if [ "$PID" == "$RunningPID" ]
            then
                echo "supervisor already start, PID:["${PID}"]"
            elif [ "$RunningPID" != "" ]
            then
                echo "supervisor start new PID:["${RunningPID}"]"
                echo "$RunningPID" > $SUPERVISORD_PIDFILE
            else
                echo "supervisor begin to start"
                $SUPERVISORD_BIN -d -c $SUPERVISORD_CONF
                exit 0
            fi
        else
          if [ "$RunningPID" == "" ]
          then
              echo "supervisor begin to start"
              $SUPERVISORD_BIN -d -c $SUPERVISORD_CONF
              exit 0
          else
              echo "supervisor already start, PID:["${PID}"]"
              echo "$RunningPID" > $SUPERVISORD_PIDFILE
          fi
        fi
    else
        RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
        if [ "$RunningPID" == "" ]
        then
            echo "supervisor begin to start"
            $SUPERVISORD_BIN -d -c $SUPERVISORD_CONF
            exit 0
        fi
    fi
}

supervisor-stop() {
    RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
    echo "stopping supervisord $RunningPID"
    kill $RunningPID
}

start() {
    echo -n "Starting $SERVICE_BIN: "
    echo "nohup $SERVICE_BIN  --conf=$SERVICE_CONF >> $DUMPFILE 2>&1 &"
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
    nohup $SERVICE_BIN --conf="$SERVICE_CONF" >> $DUMPFILE 2>&1 &
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
    if [ -f $PIDFILE ]
    then
      PID=`cat ${PIDFILE}`
    else
      PID=`ps aux|grep ${SERVICE_BIN}|grep -v grep| awk '{print $2}'`
    fi
    if [ -z "$PID" ]
    then
        echo "$SERVICE_BIN is already stopped"
    else
        kill $PID
    fi
    if [ -f $PIDFILE ]
    then
        rm $PIDFILE
    fi
}

restart() {
    stop
}

status() {
    pid=`ps -ef | grep "${SERVICE_BIN}" | grep -v grep | awk '{print $2}'`
    echo ${pid}
    if [[ -z ${pid} ]]
    then
        echo "${SERVICE_BIN} status is: stopped"
        return 1
    else
        echo "${SERVICE_BIN} status is: running, pid is ${pid}"
        return 0
    fi
}

case "$1" in
supervisor-start)
  shift
  supervisor-start
  ;;
supervisor-stop)
  shift
  supervisor-stop
  ;;
start)
    if [ "$2" == "xf" ] || [ "$2" == "xfront" ]
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

echo "Usage: $0 {start|stop|restart|status|supervisor-start|supervisor-stop machine_exporter}"

echo $SERVICE_BIN
exit 1
esac
