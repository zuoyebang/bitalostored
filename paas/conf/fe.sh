#!/usr/bin/env bash

if [ $# != 1 ] ; then
echo "USAGE: $0 RUN_MODE"
echo " e.g.: $0 start"
exit 1;
fi

CLUSTER_NAME={{.TaskExt.ClusterName}}
SERVICE_PORT={{.TaskExt.ServicePort}}

BITALOS_ADMIN="${BASH_SOURCE-$0}"
BITALOS_ADMIN="$(dirname "${BITALOS_ADMIN}")"
BITALOS_ADMIN_DIR="$(cd "${BITALOS_ADMIN}"; pwd)"

BITALOS_BIN_DIR=$BITALOS_ADMIN_DIR/bin
BITALOS_LOG_DIR=/home/homework/clog/bitalosproxy/$CLUSTER_NAME
BITALOS_CONF_DIR=$BITALOS_ADMIN_DIR/config

BITALOS_FE_BIN=$BITALOS_BIN_DIR/bitalosfe
BITALOS_FE_ASSETS_DIR=$BITALOS_BIN_DIR/dist
BITALOS_FE_PID_FILE=$BITALOS_ADMIN_DIR/bitalosfe.pid

BITALOS_FE_LOG_FILE=$BITALOS_LOG_DIR/bitalosfe.log
BITALOS_FE_DAEMON_FILE=$BITALOS_LOG_DIR/bitalosfe.out
BITALOS_FE_ADDR="0.0.0.0:${SERVICE_PORT}"
BITALOS_FE_CONF_FILE=$BITALOS_CONF_DIR/fe.toml

echo $BITALOS_FE_CONF_FILE

if [ ! -d $BITALOS_LOG_DIR ]; then
    mkdir -p $BITALOS_LOG_DIR
fi


case $1 in
start)
    echo  "starting bitalos-fe ... "
    if [ -f "$BITALOS_FE_PID_FILE" ]; then
      if kill -0 `cat "$BITALOS_FE_PID_FILE"` > /dev/null 2>&1; then
         echo $command already running as process `cat "$BITALOS_FE_PID_FILE"`.
         exit 0
      fi
    fi
    nohup "$BITALOS_FE_BIN" "--config=${BITALOS_FE_CONF_FILE}" "--assets-dir=${BITALOS_FE_ASSETS_DIR}" \
    "--log=$BITALOS_FE_LOG_FILE" "--pidfile=$BITALOS_FE_PID_FILE" "--log-level=INFO" "--listen=$BITALOS_FE_ADDR" > "$BITALOS_FE_DAEMON_FILE" 2>&1 < /dev/null &
    ;;
start-foreground)
    $BITALOS_FE_BIN "--config=${BITALOS_FE_CONF_FILE}" "--assets-dir=${BITALOS_FE_ASSETS_DIR}" \
    "--log-level=DEBUG" "--listen=$BITALOS_FE_ADDR"
    ;;
stop)
    echo "stopping bitalos-fe ... "
    if [ ! -f "$BITALOS_FE_PID_FILE" ]
    then
      echo "no bitalos-fe to stop (could not find file $BITALOS_FE_PID_FILE)"
    else
      kill -9 $(cat "$BITALOS_FE_PID_FILE")
      rm $BITALOS_FE_PID_FILE
      echo STOPPED
    fi
    exit 0
    ;;
restart)
    shift
    "$0" stop
    sleep 1
    "$0" start
    ;;
*)
    echo "Usage: $0 {start|start-foreground|stop|restart}" >&2

esac