#!/usr/bin/env bash
# 不能删，因为cron要用到

cron_file_r="/home/homework/bitalos-paas/bitalos-agent/cron.log"

t=$(date +"%Y-%m-%d %H:%M:%S")
echo "$t check agent is alive">> $cron_file_r

RunningPID=$(ps aux|grep 'bitalos-paas/bitalos-agent/bin/bitalosagent'|grep -v grep| awk '{print $2}')
if [ "$RunningPID" == "" ]
then
    echo "start bitalosagent" >> $cron_file_r
    sh /home/homework/bitalos-paas/bitalos-agent/control.sh start config.toml
else
    echo "bitalos-agent is already running" >> $cron_file_r
fi