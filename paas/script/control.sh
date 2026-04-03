#!/usr/bin/env bash

CONFIG_FILE_NAME=$2

if [ -z "$1" ]; then
  echo "Usage: stop|start|restart config文件名"
  exit
fi

if [ -z "$2" ]; then
  echo "Usage: stop|start|restart config文件名"
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

if [ -d "$ROOT_DIR/conf" ] && [ -d "$ROOT_DIR/log" ] && [ -d "$ROOT_DIR/bin" ]; then
    WORKSPACE=$ROOT_DIR
else
    WORKSPACE=$SCRIPT_DIR
fi

CONFIG=$WORKSPACE/conf/$CONFIG_FILE_NAME
PID_FILE=$WORKSPACE/pid
LOG=$WORKSPACE/log/bitalospaas
BIN=$WORKSPACE/bin/bitalospaas

start() {
        if [ ! -f "$PID_FILE" ]; then
                echo "start..."
        fi
        nohup "$BIN" --log="$LOG" --conf="$CONFIG" > boot.log 2>&1 & echo $! > "$PID_FILE"
}

stop() {
        if [ ! -f "$PID_FILE" ]; then
                echo "no pid file"
                return
        fi
        if [ ! -s "$PID_FILE" ]; then
                return
        fi
        kill "$(cat "$PID_FILE")"
}

case "$1" in
stop)
  stop
  ;;
start)
  start
  ;;
restart)
  stop
  sleep 1s
  start
  ;;
*)
  echo "Usage: $0 {stop|start|restart}"
  ;;
esac