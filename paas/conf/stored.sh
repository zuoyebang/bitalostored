#!/usr/bin/env bash

RUN_FILE="${BASH_SOURCE-$0}"
STORED_ADMIN="$(dirname "${RUN_FILE}")"
WORKING_DIR="$(cd "${STORED_ADMIN}"; pwd)"

SUPERVISORD_BIN=$WORKING_DIR/bin/supervisord
SUPERVISORD_CONF=$WORKING_DIR/config/supervisor.conf
SUPERVISORD_PIDFILE=$WORKING_DIR/supervisord-server.pid

case $1 in
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
                $SUPERVISORD_BIN -d -c $SUPERVISORD_CONF
                exit 0
            fi
        else
          if [ "$RunningPID" == "" ]
          then
              echo "supervisor server begin to start"
              $SUPERVISORD_BIN -d -c $SUPERVISORD_CONF
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
            $SUPERVISORD_BIN -d -c $SUPERVISORD_CONF
            exit 0
        fi
    fi
    ;;
 supervisor-stop)
    shift
    RunningPID=`ps aux|grep ${SUPERVISORD_CONF}|grep -v grep| awk '{print $2}'`
    echo "stopping supervisord $RunningPID"
    kill $RunningPID
    ;;
*)
    echo "unsupport operation"

esac
