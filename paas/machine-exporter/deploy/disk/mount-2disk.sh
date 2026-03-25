# 以root用户运行 只能执行一次

dev_dir=/dev

disk1=nvme0n1
disk2=nvme1n1

lsblk -l | grep -q "${disk1}"
if [ $? -ne 0 ]; then
    echo "${disk1} 不存在"
    exit 1
fi

lsblk -l | grep -q "${disk2}"
if [ $? -ne 0 ]; then
    echo "${disk2} 不存在"
    exit 1
fi
echo "${disk1} 和 ${disk2} 都存在"

# 安装mdadm 整合两块磁盘为1个磁盘阵列
yum install -y mdadm
echo "mdadm 安装完成"

# 创建Raid阵列 (磁盘名称:/dev/nvme0n1, /dev/nvme1n1)
mdadm --create --verbose /dev/md0 --level=0 --raid-devices=2 /dev/${disk1} /dev/${disk2}
mdadm --detail --scan --verbose > /etc/mdadm.conf
echo "mdadm 创建Raid阵列完成"

# 为Raid阵列创建1个分区(分区数可选 这里为1)
parted -s /dev/md0 mklabel gpt mkpart primary 0% 100%
echo "Raid阵列 /dev/md0 划分1个分区完成"

# 为分区设置文件系统为ext4格式
mkfs.ext4 /dev/md0p1
echo "Raid阵列 /dev/md0p1 格式化ext4完成"

mkdir /data
mount /dev/md0p1 /data
echo "Raid阵列 /dev/md0p1 挂载到 /data 完成"

extract_uuid_by_device() {
    local device="$1"
    if command -v lsblk >/dev/null 2>&1; then
        lsblk -o NAME,UUID | grep "$device" | head -n 1 | awk '{print $2}' | tr -d ' '
    else
        echo "Error: lsblk command not found"
        return 1
    fi
}

uuid=$(extract_uuid_by_device md0p1)
status=$?
if [ $status -eq 0 ]; then
    echo "Raid阵列 /dev/md0p1 的UUID为: $uuid"
else
    echo "查询uuid失败"
    exit 1
fi

# 将挂载信息写入 /etc/fstab，避免重启后挂载失效
echo "UUID=${uuid} /data auto rw,relatime,data=ordered 0 2" >> /etc/fstab
cat /etc/fstab

source create-user-cgroup.sh