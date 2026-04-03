#!/usr/bin/env bash

set -e

echo "========================================"
echo "PaaS 安装脚本"
echo "========================================"
echo

read -p "是否要执行 sql/mysql_init.sh 初始化 MySQL？(y/n): " init_mysql

echo

if [ "$init_mysql" = "y" ] || [ "$init_mysql" = "Y" ]; then
    echo "正在执行 MySQL 初始化..."
    if [ -f "sql/mysql_init.sh" ]; then
        chmod +x sql/mysql_init.sh
        ./sql/mysql_init.sh
        echo "MySQL 初始化完成！"
    else
        echo "错误：sql/mysql_init.sh 文件不存在！"
        exit 1
    fi
else
    echo "跳过 MySQL 初始化。"
fi

echo
read -p "是否执行 PaaS 部署？(y/n): " deploy_paas

echo

if [ "$deploy_paas" = "y" ] || [ "$deploy_paas" = "Y" ]; then
    echo "正在执行 PaaS 部署..."
    if [ -f "script/deploy_paas.sh" ]; then
        chmod +x script/deploy_paas.sh
        (cd script && ./deploy_paas.sh)
        echo "PaaS 部署完成！"
    else
        echo "错误：script/deploy_paas.sh 文件不存在！"
        exit 1
    fi
else
    echo "跳过 PaaS 部署。"
fi

echo
echo "========================================"
echo "PaaS 平台创建完成！"
echo "平台访问地址示例: http://10.116.48.58:2000/bitalospaas/static/#/"
echo "请根据实际部署环境访问相应地址"
echo "========================================"
echo

echo "请先在 PaaS 平台注册机器，然后再部署 agent！"
echo
echo "注册步骤："
echo "1. 登录 PaaS 平台"
echo "2. 进入 '控制器' 页面"
echo "3. 点击 'new machine' 按钮"
echo "4. 填写机器信息并保存"
echo
echo "注册完成后按 Enter 键继续..."
read

echo
read -p "是否部署 agent？(y/n): " deploy_agent

echo

if [ "$deploy_agent" = "y" ] || [ "$deploy_agent" = "Y" ]; then
    echo "正在检查 agent 配置文件..."
    if [ -f "conf/bitalosagent.toml" ]; then
        echo "agent 配置文件存在，正在验证配置..."
        # 输出配置文件内容
        echo "配置文件内容："
        echo "========================================"
        cat conf/bitalosagent.toml
        echo "========================================"
        
        # 人工确认配置文件
        while true; do
            read -p "配置文件内容是否正确？(y/n): " confirm
            case "$confirm" in
                [Yy])
                    echo "配置文件确认通过！"
                    break
                    ;;
                [Nn])
                    echo "请先修改配置文件后再继续..."
                    exit 1
                    ;;
                *)
                    echo "错误：请输入 y 或 n"
                    ;;
            esac
        done
    else
        echo "错误：conf/bitalosagent.toml 配置文件不存在！"
        exit 1
    fi
    
    # 执行 agent 部署脚本
    echo "正在部署 agent..."
    if [ -f "agent/script/deploy_agent.sh" ]; then
        chmod +x agent/script/deploy_agent.sh
        (cd agent/script && ./deploy_agent.sh)
        echo "agent 部署完成！"
    else
        echo "错误：agent/script/deploy_agent.sh 文件不存在！"
        exit 1
    fi
else
    echo "跳过 agent 部署。"
fi

echo
echo "========================================"
echo "所有安装步骤已完成！"
echo "========================================"