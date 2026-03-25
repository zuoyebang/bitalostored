# (第一步) 将所有文件上传到任意指定的目录 手动操作

- 方式1: rz
- 方式2: scp -r {ip1}:{path} {ip2}:{path}

# (第二步) 将第一步的目录文件拷贝到工作目录

1, 在目标机器上执行cp-file.sh

修改配置文件: /data/homework/prometheus/machine-exporter/conf/machine-exporter.toml

2, 编译machine-exporter

```
cd /home/homework/prometheus/machine-exporter
make
```

或者将某个已经编译好的二进制拷贝到目录: /data/homework/prometheus/machine-exporter/bin

# (第三步) 配置和环境检查

检查二进制是否存在，配置文件是否存在，配置文件输出到标准输出，检查进程是否已启动

在目标机器上执行 check-env.sh

# (第四步) 启动/停止进程

在目标机器上执行

```
cd /home/homework/prometheus/machine-exporter/bin
bash load.sh {start|stop|restart|status|supervisor-start|supervisor-stop}
```