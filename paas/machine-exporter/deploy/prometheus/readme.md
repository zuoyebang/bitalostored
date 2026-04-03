# 1 安装和启动prometheus

安装步骤:

1, 下载文件 https://github.com/prometheus/prometheus/releases/download/v3.10.0/prometheus-3.10.0.linux-amd64.tar.gz

2, 将该文件解压到安装路径: /data/homework/prometheus/prometheus-2.14.0

3, 修改配置文件: /data/homework/prometheus/prometheus-2.14.0/prometheus.yml

修改内容: 127.0.0.1:9839 替换为 machine-exporter所对应的ip和端口

4 启动/停止进程

在目标机器上执行

```
cd /data/homework/prometheus/prometheus-2.14.0
bash load.sh {start|stop|restart|status}
```