#!/bin/bash

# root 用户执行

createUserHomework() {
    groupadd homework
    useradd -g homework homework
    passwd homework //修改密码
    mkdir /data/homework
    cp /home/homework/.bash* /data/homework/ //将.bash等文件复制过去
    chown -R homework:homework /data/homework
    # 修改/etc/passwd中homework的默认登陆目录，改成/data/homework
    usermod -d /data/homework homework
    rm -rf /home/homework
    ln -s /data/homework /home/homework
    chown -R homework:homework /home/homework
}

createUserHomework
echo "homework 用户创建完成"

createCgroup() {
    cd /sys/fs/cgroup/cpu/ && mkdir -p stored && chown -R homework:homework stored
    cd /sys/fs/cgroup/cpuset/ && mkdir -p stored && chown -R homework:homework stored
}

createCgroup
echo "cgroup 创建完成"

handleSystemRestart() {
    echo "sleep 60" >> /etc/rc.local
    echo "/data/homework/bitalos-paas/bitalos-agent/storedcpu.sh" >> /etc/rc.local
    echo "su -l homework -c '/home/homework/bitalos-paas/bitalos-agent/recovery.sh' " >> /etc/rc.local
}

handleSystemRestart
echo "系统重启脚本添加完成"