#!/usr/bin/env bash
#文件名称和路径不能改，在/etc/rc.local中执行

cd /sys/fs/cgroup/cpu/ && mkdir -p stored && chown -R homework:homework stored
cd /sys/fs/cgroup/cpuset/ && mkdir -p stored && chown -R homework:homework stored