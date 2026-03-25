# 1 下载grafana rpm包，假设系统为centos

```
wget https://dl.grafana.com/grafana/release/12.4.0/grafana_12.4.0_22325204712_linux_amd64.rpm

# 切换成root账户
yum -y localinstall grafana_12.4.0_22325204712_linux_amd64.rpm

# 启动grafana-server
systemctl start grafana-server

# 检查grafana-server是否已启动
systemctl status grafana-server

# 停止grafana-server
systemctl stop grafana-server

# 配置数据源
新增Prometheus数据源: Connections -> Data sources

Name: Prometheus-Stored
Prometheus server URL: http://{prometheus-stored}:9090

# 将json文件导入到dashboard中   
新增Dashboard: Dashboards -> New -> Import dashboard

Upload JSON File: 选择cluster.json

展示不出来，点击某个面板修改下面板设置；如果不行，再尝试修改uid

```