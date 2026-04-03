# 为Raid阵列创建1个分区(分区数可选 这里为1)
disk="" #示例:md0
partdisk="" #示例:md0p1

if [ -z "$disk" ] || [ -z "$partdisk" ]; then
    echo "请先设置 disk 和 partdisk 变量"
    exit 1
fi

parted -s /dev/${disk} mklabel gpt mkpart primary 0% 100%
echo "Raid阵列 /dev/${disk} 划分1个分区完成"

# 为分区设置文件系统为ext4格式
mkfs.ext4 /dev/${partdisk}
echo "Raid阵列 /dev/${partdisk} 格式化ext4完成"

mkdir /data
mount /dev/${partdisk} /data
echo "Raid阵列 /dev/${partdisk} 挂载到 /data 完成"

extract_uuid_by_device() {
    local device="$1"
    if command -v lsblk >/dev/null 2>&1; then
        lsblk -o NAME,UUID | grep "$device" | head -n 1 | awk '{print $2}' | tr -d ' '
    else
        echo "Error: lsblk command not found"
        return 1
    fi
}

uuid=$(extract_uuid_by_device ${partdisk})
status=$?
if [ $status -eq 0 ]; then
    echo "Raid阵列 /dev/${partdisk} 的UUID为: $uuid"
else
    echo "查询uuid失败"
    exit 1
fi

# 将挂载信息写入 /etc/fstab，避免重启后挂载失效
echo "UUID=${uuid} /data auto rw,relatime,data=ordered 0 2" >> /etc/fstab
cat /etc/fstab

source create-user-cgroup.sh