#!/usr/bin/env bash

# 设置错误处理
set -euo pipefail

# 变量定义
ROOT_PATH=/home/homework
DEPLOY_PATH=/home/homework/bitalos-paas/bitalos-paas
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$SCRIPT_DIR/.."
BUILD=false
SERVER_IP=""

# 显示帮助信息
show_help() {
    echo "使用方法: $0 [选项]"
    echo "选项:"
    echo "  -h, --help         显示帮助信息"
    echo ""
    echo "注意: 本脚本使用交互式输入方式运行"
}

# 解析命令行参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "错误: 未知参数 '$1'"
                show_help
                exit 1
                ;;
        esac
    done
}

# 获取用户输入
get_user_input() {
    # 获取部署类型
    while true; do
        echo "请选择部署类型:"
        echo "1. 仅部署前端 (FE)"
        echo "2. 仅部署后端 (PAAS)"
        echo "3. 同时部署前端和后端 (ALL)"
        read -r -p "请输入选项 (1-3): " DEPLOY_TYPE
        case "$DEPLOY_TYPE" in
            1)
                DEPLOY_FE=true
                DEPLOY_PAAS=false
                break
                ;;
            2)
                DEPLOY_FE=false
                DEPLOY_PAAS=true
                break
                ;;
            3)
                DEPLOY_FE=true
                DEPLOY_PAAS=true
                break
                ;;
            *)
                echo "错误: 请输入 1-3 之间的数字"
                ;;
        esac
    done

    # 获取服务器地址
    while true; do
        read -r -p "请输入服务器地址: " SERVER_IP_INPUT
        if [[ -n "$SERVER_IP_INPUT" ]]; then
            SERVER_IP="$SERVER_IP_INPUT"
            break
        else
            echo "错误: 服务器地址不能为空"
        fi
    done

    # 如果部署后端，获取编译选项
    if [[ "$DEPLOY_PAAS" == true ]]; then
        while true; do
            read -r -p "是否编译代码？(y/n) :" BUILD_INPUT
            case "$BUILD_INPUT" in
                [Yy])
                    BUILD=true
                    break
                    ;;
                [Nn])
                    BUILD=false
                    break
                    ;;
                *)
                    echo "错误: 请输入 y 或 n"
                    ;;
            esac
        done
    fi
}

# 验证必要文件和目录
validate_files() {
    echo "验证必要文件..."
    
    if [[ "$DEPLOY_PAAS" == true ]]; then
        if [[ ! -f "$PROJECT_ROOT/script/control.sh" ]]; then
            echo "错误: control.sh 文件不存在"
            exit 1
        fi
        
        if [[ "$BUILD" == true ]]; then
            if [[ ! -f "$PROJECT_ROOT/main.go" ]]; then
                echo "错误: main.go 文件不存在"
                exit 1
            fi
        else
            if [[ ! -f "$PROJECT_ROOT/bin/bitalospaas" ]]; then
                echo "警告: bitalospaas 可执行文件不存在，建议选择编译选项"
            fi
        fi

        CONF_FILE="$PROJECT_ROOT/conf/bitalospaas.toml"
        if [[ ! -f "$CONF_FILE" ]]; then
            echo "错误: 配置文件 '$CONF_FILE' 不存在"
            exit 1
        fi
    fi
    
    echo "文件验证通过！"
}

# 验证服务器目录
test_server_dir() {
    echo "验证服务器目录..."
    if [[ "$DEPLOY_FE" == true || "$DEPLOY_PAAS" == true ]]; then
        ssh homework@$SERVER_IP "if [ ! -d '$ROOT_PATH' ]; then mkdir -p '$ROOT_PATH'; echo '创建目录成功'; else echo '目录已存在'; fi"
        if [[ $? -ne 0 ]]; then
            echo "错误: 验证服务器目录失败"
            exit 1
        fi
    fi
    echo "服务器目录验证通过！"
}

# 编译代码
build_code() {
    if [[ "$DEPLOY_PAAS" == true && "$BUILD" == true ]]; then
        echo "开始编译代码..."
        mkdir -p "$PROJECT_ROOT/bin"
        go build -o "$PROJECT_ROOT/bin/bitalospaas" "$PROJECT_ROOT/main.go"
        echo "编译成功！"
    fi
}

# 部署前端
deploy_fe() {
    if [[ "$DEPLOY_FE" == true ]]; then
        echo "开始部署前端..."
        cd "$PROJECT_ROOT" || exit
        zip -r bitalospaasfe.zip fe
        mv bitalospaasfe.zip script/
        cd script || exit

        echo "开始部署前端到服务器..."
        ssh homework@$SERVER_IP "mkdir -p $DEPLOY_PATH"
        scp bitalospaasfe.zip homework@$SERVER_IP:$DEPLOY_PATH
        ssh homework@$SERVER_IP "cd $DEPLOY_PATH; rm -rf fe; unzip bitalospaasfe.zip; rm -f bitalospaasfe.zip"
        rm -f bitalospaasfe.zip
        echo "前端部署成功！"
    fi
}

# 打包后端文件
package_files() {
    if [[ "$DEPLOY_PAAS" == true ]]; then
        echo "开始打包后端文件..."
        rm -rf src
        mkdir -p src/conf src/log src/bin
        cp "$PROJECT_ROOT/bin/bitalospaas" src/bin/
        cp "$PROJECT_ROOT/script/control.sh" src/
        cp "$CONF_FILE" src/conf/bitalospaas.toml
        tar -czvf bitalospaas.tar.gz src
        echo "打包成功！"
    fi
}

# 部署后端到服务器
deploy_to_server() {
    if [[ "$DEPLOY_PAAS" == true ]]; then
        local server_ip="$1"
        echo "部署后端到服务器 $server_ip..."
        scp bitalospaas.tar.gz install_paas.sh homework@"$server_ip":"$DEPLOY_PATH"
        ssh homework@"$server_ip" "cd '$DEPLOY_PATH'; sh install_paas.sh"
        echo "部署到 $server_ip 成功！"
    fi
}

# 部署后端环境
deploy_paas() {
    if [[ "$DEPLOY_PAAS" == true ]]; then
        deploy_to_server "$SERVER_IP"
        echo "后端部署成功！"
    fi
}

# 清理临时文件
cleanup() {
    echo "清理临时文件..."
    if [[ "$DEPLOY_PAAS" == true ]]; then
        rm -rf src
        rm -f bitalospaas.tar.gz
    fi
    echo "清理完成！"
}

# 主函数
main() {
    # 检查执行目录
    if [[ "$SCRIPT_DIR" != "." ]]; then
        echo "请进入script目录执行该脚本"
        exit 1
    fi

    echo "===== 部署脚本 ====="

    # 解析参数
    parse_args "$@"

    # 获取用户输入
    get_user_input

    # 验证文件
    validate_files

    # 验证服务器目录
    test_server_dir

    # 编译代码
    build_code

    # 部署前端
    deploy_fe

    # 确认配置文件内容已更改
    if [[ "$DEPLOY_PAAS" == true ]]; then
        echo ""
        echo "========================================="
        echo "请确认配置文件内容已正确更改"
        echo "配置文件路径: $CONF_FILE"
        echo "========================================="
        echo ""
        echo "配置文件内容预览:"
        echo "-----------------------------------------"
        cat "$CONF_FILE"
        echo "-----------------------------------------"
        echo ""
        
        while true; do
            read -r -p "配置文件内容是否已正确更改? (y/n): " CONFIRM_CONF
            case "$CONFIRM_CONF" in
                y|Y|yes|YES)
                    echo "配置文件确认通过，继续部署..."
                    break
                    ;;
                n|N|no|NO)
                    echo "请先修改配置文件后重新运行部署脚本"
                    exit 0
                    ;;
                *)
                    echo "无效输入，请输入 y 或 n"
                    ;;
            esac
        done
    fi

    # 打包后端文件
    package_files

    # 部署后端
    deploy_paas

    # 清理临时文件
    cleanup

    echo "===== 部署完成 ====="
}

# 执行主函数
main "$@"
