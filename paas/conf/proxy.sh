#!/usr/bin/env bash

if [ $# != 1 ] ; then
echo "USAGE: $0 RUN_MODE"
echo " e.g.: $0 start-foreground"
exit 1;
fi

CLUSTER_NAME={{.TaskExt.ClusterName}}

BITALOS_ADMIN="${BASH_SOURCE-$0}"
BITALOS_ADMIN="$(dirname "${BITALOS_ADMIN}")"
BITALOS_ADMIN_DIR="$(cd "${BITALOS_ADMIN}"; pwd)"

BITALOS_BIN_DIR=$BITALOS_ADMIN_DIR/bin
BITALOS_LOG_DIR=/home/homework/clog/bitalosproxy/$CLUSTER_NAME
BITALOS_CONF_DIR=$BITALOS_ADMIN_DIR/config

BITALOS_PROXY_BIN=$BITALOS_BIN_DIR/bitalosproxy
BITALOS_PROXY_PID_FILE=$BITALOS_ADMIN_DIR/bitalosproxy.pid

BITALOS_PROXY_LOG_FILE=$BITALOS_LOG_DIR/bitalos-proxy.log
BITALOS_PROXY_DAEMON_FILE=$BITALOS_LOG_DIR/bitalos-proxy.out

BITALOS_DASHBOARD_ADDR={{.TaskExt.DashboardAddress}}

SUPERVISORD_BIN=$BITALOS_BIN_DIR/supervisord
SUPERVISORD_CONF=$BITALOS_CONF_DIR/supervisor.conf
SUPERVISORD_PIDFILE=$BITALOS_ADMIN_DIR/supervisord-proxy.pid

echo $BITALOS_PROXY_CONF_FILE

if [ ! -d $BITALOS_LOG_DIR ]; then
    mkdir -p $BITALOS_LOG_DIR
fi

BITALOS_PROXY_CONF_FILE=$BITALOS_CONF_DIR/proxy-{{.TaskExt.CloudType}}.toml

export GOGC=300

case $1 in
start)
    echo  "starting bitalos-proxy ... "
    if [ -f "$BITALOS_PROXY_PID_FILE" ]; then
      if kill -0 `cat "$BITALOS_PROXY_PID_FILE"` > /dev/null 2>&1; then
         echo $command already running as process `cat "$BITALOS_PROXY_PID_FILE"`.
         exit 0
      fi
    fi
    nohup "$BITALOS_PROXY_BIN" "--config=${BITALOS_PROXY_CONF_FILE}" "--dashboard=${BITALOS_DASHBOARD_ADDR}" \
    "--pidfile=$BITALOS_PROXY_PID_FILE" >> "$BITALOS_PROXY_DAEMON_FILE" 2>&1 < /dev/null &
    ;;
start-foreground)
    $BITALOS_PROXY_BIN "--config=${BITALOS_PROXY_CONF_FILE}" "--dashboard=${BITALOS_DASHBOARD_ADDR}" \
    "--pidfile=$BITALOS_PROXY_PID_FILE" >> "$BITALOS_PROXY_DAEMON_FILE" 2>&1 < /dev/null
    ;;
stop)
    echo "stopping bitalos-proxy ... "
    if [ ! -f "$BITALOS_PROXY_PID_FILE" ]
    then
      RunningPID=`ps aux|grep ${BITALOS_PROXY_LOG_FILE}|grep -v grep| awk '{print $2}'`
      if [ "$RunningPID" == "" ]
      then
        echo "no bitalos-proxy to stop (could not find file $BITALOS_PROXY_PID_FILE)"
      else
        kill -2 $RunningPID
        echo STOPPED
      fi
    else
      kill -2 $(cat "$BITALOS_PROXY_PID_FILE")
      echo STOPPED
    fi
    exit 0
    ;;
stop-forced)
    echo "stopping bitalos-proxy ... "
    if [ ! -f "$BITALOS_PROXY_PID_FILE" ]
    then
      RunningPID=`ps aux|grep ${BITALOS_PROXY_LOG_FILE}|grep -v grep| awk '{print $2}'`
      if [ "$RunningPID" == "" ]
      then
        echo "no bitalos-proxy to stop (could not find file $BITALOS_PROXY_PID_FILE)"
      else
        kill -2 $RunningPID
        echo STOPPED
      fi
    else
      kill -9 $(cat "$BITALOS_PROXY_PID_FILE")
      rm "$BITALOS_PROXY_PID_FILE"
      echo STOPPED
    fi
    exit 0
    ;;
supervisor-start)
    shift
    echo -n $"check supervisor is running"
    if [ -f $SUPERVISORD_PIDFILE ]
    then
        PID=`cat ${SUPERVISORD_PIDFILE}`
        RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
        if [ "$PID" != "" ]
        then
            if [ "$PID" == "$RunningPID" ]
            then
                echo "supervisor server already start, PID:["${PID}"]"
            elif [ "$RunningPID" != "" ]
            then
                echo "supervisor server start new PID:["${RunningPID}"]"
                echo "$RunningPID" > $SUPERVISORD_PIDFILE
            else
                echo "supervisor server begin to start"
                $SUPERVISORD_BIN -c $SUPERVISORD_CONF -d
                exit 0
            fi
        else
          if [ "$RunningPID" == "" ]
          then
              echo "supervisor server begin to start"
              $SUPERVISORD_BIN -c $SUPERVISORD_CONF -d
              exit 0
          else
              echo "supervisor server already start, PID:["${PID}"]"
              echo "$RunningPID" > $SUPERVISORD_PIDFILE
          fi
        fi
    else
        RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
        if [ "$RunningPID" == "" ]
        then
            echo "supervisor server begin to start"
            $SUPERVISORD_BIN -c $SUPERVISORD_CONF -d
            exit 0
        fi
    fi
    ;;
supervisor-stop)
  shift
  RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
  echo "stopping supervisord"
  kill -9 $RunningPID
  sleep 1
  "$0" stop
  ;;
restart)
    shift
    "$0" stop
    sleep 1
    "$0" start
    ;;
*)
    echo "Usage: $0 {start|start-foreground|supervisor-start|supervisor-stop|stop|stop-forced|restart}" >&2

esac
