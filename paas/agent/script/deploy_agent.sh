#!/usr/bin/env bash

REMOTE_USER="homework"
REMOTE_BASE_DIR="/home/homework"
REMOTE_AGENT_DIR="$REMOTE_BASE_DIR/bitalos-paas/bitalos-agent"
REMOTE_INSTALL_SCRIPT="$REMOTE_AGENT_DIR/install_agent.sh"
LOCAL_INSTALL_SCRIPT="./script/install_agent.sh"

if [ "$(dirname "$0")" != "." ]; then
    echo "请进入script目录执行该脚本"
    exit 1
fi

echo "=========================================="
echo "      Bitalos Agent 部署脚本"
echo "=========================================="

read -r -p "是否编译代码？(y/n) [默认: n]: " build
build=${build:-n}

if [ "$build" != "y" ] && [ "$build" != "n" ]; then
    echo "错误：请输入 y 或 n"
    exit 1
fi

SCRIPT_PATH=$(cd "$(dirname "$0")" || exit; pwd)

# 打印当前路径
echo "当前路径: $SCRIPT_PATH"
echo "IP文件应放在此路径下"

read -r -p "请输入当前路径的IP文件名: " filename
if [ -z "$filename" ]; then
    echo "错误：IP文件名不能为空"
    exit 1
fi

WORK_DIR=$SCRIPT_PATH/../
cd "$WORK_DIR" || exit

if [ "$build" == "y" ]; then
    echo "----------------------------------------"
    echo "开始编译代码..."
    echo "----------------------------------------"
    bash version
    go build -v -o bin/bitalosagent main.go
    if [ $? -eq 0 ]; then
        echo "编译成功！"
    else
        echo "编译失败！"
        exit 1
    fi
fi

fullFile="$SCRIPT_PATH"/"${filename}"
if [ ! -f "$fullFile" ]; then
    echo "错误：IP文件不存在！路径: $fullFile"
    exit 1
fi

if [ -f "bitalosagent.tar.gz" ]; then
    echo "删除旧的打包文件: bitalosagent.tar.gz"
    rm -f bitalosagent.tar.gz
fi

echo "----------------------------------------"
echo "开始打包..."
echo "----------------------------------------"

# 检查配置文件位置
if [ -f "$WORK_DIR/conf/bitalosagent.toml" ]; then
    echo "使用当前目录的 conf 目录"
    tar -zcvf bitalosagent.tar.gz -C"$WORK_DIR" bin conf -C"$WORK_DIR/internal" lib -C"$WORK_DIR/script" recovery.sh control.sh storedcpu.sh start.sh
elif [ -f "$WORK_DIR/../conf/bitalosagent.toml" ]; then
    echo "使用上一级的 conf/bitalosagent.toml 文件"
    mkdir -p "$WORK_DIR/conf"
    cp "$WORK_DIR/../conf/bitalosagent.toml" "$WORK_DIR/conf/"
    tar -zcvf bitalosagent.tar.gz -C"$WORK_DIR" bin conf -C"$WORK_DIR/internal" lib -C"$WORK_DIR/script" recovery.sh control.sh storedcpu.sh start.sh
    rm -f "$WORK_DIR/conf/bitalosagent.toml"
else
    echo "错误：找不到配置文件！"
    echo "请确保以下位置之一存在配置文件："
    echo "  1. $WORK_DIR/conf/bitalosagent.toml"
    echo "  2. $WORK_DIR/../conf/bitalosagent.toml"
    exit 1
fi

if [ $? -ne 0 ]; then
    echo "错误：打包失败！"
    exit 1
fi

echo "----------------------------------------"
echo "开始部署..."
echo "----------------------------------------"

success_count=0
fail_count=0
failed_ips=()

read -r -p "如果远程目录不存在，是否自动创建？(y/n) [默认: y]: " auto_create
auto_create=${auto_create:-y}

for ip in $(cat "$fullFile"); do
    echo ""
    echo "=========================================="
    echo "部署到服务器: $ip"
    echo "=========================================="
    
    if [ -z "$ip" ]; then
        continue
    fi
    
    echo "检查远程目录: $REMOTE_AGENT_DIR"
    dir_exists=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$REMOTE_USER@$ip" "test -d $REMOTE_AGENT_DIR && echo 'exists' || echo 'not_exists'" 2>/dev/null || echo "connection_failed")
    
    if [ "$dir_exists" == "connection_failed" ]; then
        echo "错误：无法连接到服务器 $ip"
        ((fail_count++))
        failed_ips+=("$ip (连接失败)")
        continue
    fi
    
    if [ "$dir_exists" == "not_exists" ]; then
        echo "警告：目录 $REMOTE_AGENT_DIR 不存在"
        
        if [ "$auto_create" == "y" ]; then
            echo "尝试创建目录..."
            ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$ip" "mkdir -p $REMOTE_AGENT_DIR" 2>/dev/null
            
            if [ $? -eq 0 ]; then
                echo "目录创建成功"
            else
                echo "错误：目录创建失败"
                ((fail_count++))
                failed_ips+=("$ip (目录创建失败)")
                continue
            fi
        else
            read -r -p "是否在 $ip 上创建目录 $REMOTE_AGENT_DIR？(y/n) [默认: y]: " create_dir
            create_dir=${create_dir:-y}
            
            if [ "$create_dir" == "y" ]; then
                echo "创建目录..."
                ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$ip" "mkdir -p $REMOTE_AGENT_DIR" 2>/dev/null
                
                if [ $? -eq 0 ]; then
                    echo "目录创建成功"
                else
                    echo "错误：目录创建失败"
                    ((fail_count++))
                    failed_ips+=("$ip (目录创建失败)")
                    continue
                fi
            else
                echo "跳过服务器 $ip"
                continue
            fi
        fi
    else
        echo "目录已存在"
    fi
    
    echo "上传 agent 包..."
    scp -o StrictHostKeyChecking=no bitalosagent.tar.gz "$REMOTE_USER@$ip:$REMOTE_AGENT_DIR/" 2>/dev/null
    
    if [ $? -ne 0 ]; then
        echo "错误：上传 agent 包失败"
        ((fail_count++))
        failed_ips+=("$ip (上传失败)")
        continue
    fi
    
    echo "上传安装脚本..."
    scp -o StrictHostKeyChecking=no "$LOCAL_INSTALL_SCRIPT" "$REMOTE_USER@$ip:$REMOTE_AGENT_DIR/" 2>/dev/null
    
    if [ $? -ne 0 ]; then
        echo "错误：上传安装脚本失败"
        ((fail_count++))
        failed_ips+=("$ip (上传脚本失败)")
        continue
    fi
    
    echo "执行安装脚本..."
    ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$ip" "$REMOTE_INSTALL_SCRIPT" &
    
    if [ $? -eq 0 ]; then
        echo "部署命令已发送到 $ip"
        ((success_count++))
    else
        echo "错误：执行安装脚本失败"
        ((fail_count++))
        failed_ips+=("$ip (安装失败)")
    fi
done

echo ""
echo "=========================================="
echo "部署完成！"
echo "=========================================="
echo "成功: $success_count 台"
echo "失败: $fail_count 台"

if [ $fail_count -gt 0 ]; then
    echo ""
    echo "失败的服务器列表:"
    for failed_ip in "${failed_ips[@]}"; do
        echo "  - $failed_ip"
    done
fi

echo "=========================================="