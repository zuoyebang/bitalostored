#!/usr/bin/env bash

if [ $# != 1 ] ; then
echo "USAGE: $0 RUN_MODE"
echo " e.g.: $0 start"
exit 1;
fi

CLUSTER_NAME={{.TaskExt.ClusterName}}

BITALOS_ADMIN="${BASH_SOURCE-$0}"
BITALOS_ADMIN="$(dirname "${BITALOS_ADMIN}")"
BITALOS_ADMIN_DIR="$(cd "${BITALOS_ADMIN}"; pwd)"

BITALOS_BIN_DIR=$BITALOS_ADMIN_DIR/bin
BITALOS_LOG_DIR=/home/homework/clog/bitalos-dashboard/$CLUSTER_NAME
BITALOS_CONF_DIR=$BITALOS_ADMIN_DIR/config

BITALOS_DASHBOARD_BIN=$BITALOS_BIN_DIR/bitalosdashboard
BITALOS_DASHBOARD_PID_FILE=$BITALOS_ADMIN_DIR/bitalosdashboard.pid

BITALOS_DASHBOARD_LOG_FILE=$BITALOS_LOG_DIR/bitalosdashboard.log
BITALOS_DASHBOARD_DAEMON_FILE=$BITALOS_LOG_DIR/bitalosdashboard.out

BITALOS_DASHBOARD_CONF_FILE=$BITALOS_CONF_DIR/dashboard.toml

echo $BITALOS_DASHBOARD_CONF_FILE

if [ ! -d $BITALOS_LOG_DIR ]; then
    mkdir -p $BITALOS_LOG_DIR
fi

export GOGC=300

case $1 in
start)
    echo  "starting bitalos-dashboard ... "
    if [ -f "$BITALOS_DASHBOARD_PID_FILE" ]; then
      if kill -0 `cat "$BITALOS_DASHBOARD_PID_FILE"` > /dev/null 2>&1; then
         echo $command already running as process `cat "$BITALOS_DASHBOARD_PID_FILE"`.
         exit 0
      fi
    fi
    nohup "$BITALOS_DASHBOARD_BIN" "--config=${BITALOS_DASHBOARD_CONF_FILE}"  "--database=database" \
    "--log=$BITALOS_DASHBOARD_LOG_FILE" "--log-level=INFO" "--pidfile=$BITALOS_DASHBOARD_PID_FILE" > "$BITALOS_DASHBOARD_DAEMON_FILE" 2>&1 < /dev/null &
    ;;
start-foreground)
    "$BITALOS_DASHBOARD_BIN" "--config=${BITALOS_DASHBOARD_CONF_FILE}" \
    "--log=$BITALOS_DASHBOARD_LOG_FILE" "--log-level=INFO" "--pidfile=$BITALOS_DASHBOARD_PID_FILE" > "$BITALOS_DASHBOARD_DAEMON_FILE" 2>&1 < /dev/null
    ;;
stop)
    echo "stopping bitalos-dashboard ... "
    if [ ! -f "$BITALOS_DASHBOARD_PID_FILE" ]
    then
      echo "no pid file"
      return
    else
      kill -2 $(cat "$BITALOS_DASHBOARD_PID_FILE")
      echo STOPPED
    fi
    exit 0
    ;;
stop-forced)
    echo "stopping bitalos-dashboard ... "
    if [ ! -f "$BITALOS_DASHBOARD_PID_FILE" ]
    then
      RunningPID=`ps aux|grep ${BITALOS_DASHBOARD_LOG_FILE}|grep -v grep| awk '{print $2}'`
      if [ "$RunningPID" == "" ]
      then
        echo "no bitalos-dashboard to stop (could not find file $BITALOS_DASHBOARD_PID_FILE)"
      else
        kill -2 $RunningPID
        echo STOPPED
      fi
    else
      kill -9 $(cat "$BITALOS_DASHBOARD_PID_FILE")
      rm "$BITALOS_DASHBOARD_PID_FILE"
      echo STOPPED
    fi
    exit 0
    ;;
supervisor-restart)
    shift
    "$0" stop
    sleep 1
    "$0" start-foreground
    ;;
restart)
    shift
    "$0" stop
    sleep 1
    "$0" start
    ;;
*)
    echo "Usage: $0 {start|start-foreground|stop|stop-forced|restart}" >&2

esac