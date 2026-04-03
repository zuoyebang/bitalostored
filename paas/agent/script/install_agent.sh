#!/usr/bin/env bash

agent_path="/home/homework/bitalos-paas/bitalos-agent"
cron_file="${agent_path}/cron.list"
mkdir -p $agent_path

function add_agent_cron() {
  old_cron=`crontab -l`
  command="bash /home/homework/bitalos-paas/bitalos-agent/start.sh >> ${agent_path}/cron.log"
  agent_cron="*/1 * * * * ${command}"
  if [[ $old_cron =~ $command ]]
  then
          echo "agent cron exist"
  else
          echo "$old_cron" > $cron_file
          echo "$agent_cron" >> $cron_file
          crontab $cron_file

          echo "agent cron no exist. cron added"
  fi
}

echo "please check disk mount dir at first time installing agent \n "
df -h

cd $agent_path

if [ -f "bitalosagent.tar.gz" ]; then
  tar -xf bitalosagent.tar.gz
fi

if [ -f "$agent_path/lib/lib-stored.tar.gz" ]; then
  cd $agent_path/lib
  tar -xf lib-stored.tar.gz
  sleep 1
  rm -f lib-stored.tar.gz
fi

echo "restart bitalos-agent program. wait..."
cd $agent_path; mkdir log; rm -f bitalosagent.tar.gz; ./control.sh start bitalosagent.toml

echo "check [homework] crontab. bitalos-agent start cron exist"
add_agent_cron