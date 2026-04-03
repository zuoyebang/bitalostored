#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

if [ -d "$SCRIPT_DIR/conf" ] && [ -d "$SCRIPT_DIR/log" ] && [ -d "$SCRIPT_DIR/bin" ]; then
    WORKSPACE=$SCRIPT_DIR
else
    WORKSPACE=$(cd "$SCRIPT_DIR/.." && pwd)
fi

CONFIG_DIR="$WORKSPACE/conf"
PID_FILE="$WORKSPACE/pid"
LOG="$WORKSPACE/log/bitalosagent"
BIN="$WORKSPACE/bin/bitalosagent"

# 启动函数，支持recovery模式
start() {
    local recovery_flag=""
    local need_confirm=true
    
    if [ "$1" == "recovery" ]; then
        recovery_flag="--recovery=start"
        if [ "$2" == "no_confirm" ]; then
            need_confirm=false
        fi
    elif [ "$1" == "no_confirm" ]; then
        need_confirm=false
    fi
    
    if [ ! -f "$PID_FILE" ]; then
        echo "启动中..."
    fi
    echo "将要执行的命令:"
    echo "nohup \"$BIN\" --log=\"$LOG\" --conf=\"$CONFIG\" $recovery_flag > \"$WORKSPACE/boot.log\" 2>&1 & echo $! > \"$PID_FILE\""
    
    if [ "$need_confirm" == true ]; then
        read -p "确认执行？(y/n): " confirm
        if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
            echo "取消执行"
            return
        fi
    fi
    
    nohup "$BIN" --log="$LOG" --conf="$CONFIG" $recovery_flag > "$WORKSPACE/boot.log" 2>&1 & echo $! > "$PID_FILE"
    echo "服务已启动"
}

# 命令行模式处理
if [ $# -ge 1 ]; then
    case "$1" in
        stop)
            if [ -f "$PID_FILE" ] && [ -s "$PID_FILE" ]; then
                pid=$(cat "$PID_FILE")
                echo "将要执行的命令:"
                echo "kill $pid"
                kill "$pid"
                echo "服务已停止"
            else
                echo "没有找到PID文件或PID文件为空"
            fi
            exit 0
            ;;
        start)
            if [ -n "$2" ]; then
                CONFIG_FILE_NAME="$2"
                CONFIG="$CONFIG_DIR/$CONFIG_FILE_NAME"
                if [ -f "$CONFIG" ]; then
                    start no_confirm
                else
                    echo "错误: 配置文件 $CONFIG 不存在"
                    exit 1
                fi
            else
                echo "Usage: $0 start config文件名"
                exit 1
            fi
            exit 0
            ;;
        restart)
            if [ -n "$2" ]; then
                CONFIG_FILE_NAME="$2"
                CONFIG="$CONFIG_DIR/$CONFIG_FILE_NAME"
                if [ -f "$CONFIG" ]; then
                    echo "重启服务..."
                    echo "将先停止服务，然后启动服务"
                    if [ -f "$PID_FILE" ] && [ -s "$PID_FILE" ]; then
                        pid=$(cat "$PID_FILE")
                        kill "$pid"
                        sleep 2
                    fi
                    start no_confirm
                else
                    echo "错误: 配置文件 $CONFIG 不存在"
                    exit 1
                fi
            else
                echo "Usage: $0 restart config文件名"
                exit 1
            fi
            exit 0
            ;;
        recovery)
            if [ -n "$2" ]; then
                CONFIG_FILE_NAME="$2"
                CONFIG="$CONFIG_DIR/$CONFIG_FILE_NAME"
                if [ -f "$CONFIG" ]; then
                    start recovery no_confirm
                else
                    echo "错误: 配置文件 $CONFIG 不存在"
                    exit 1
                fi
            else
                echo "Usage: $0 recovery config文件名"
                exit 1
            fi
            exit 0
            ;;
        *)
            echo "Usage: $0 {stop|start|restart|recovery} config文件名"
            exit 1
            ;;
    esac
fi

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "没有找到PID文件"
        return
    fi
    if [ ! -s "$PID_FILE" ]; then
        echo "PID文件为空"
        return
    fi
    pid=$(cat "$PID_FILE")
    echo "将要执行的命令:"
    echo "kill $pid"
    read -p "确认执行？(y/n): " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        echo "取消执行"
        return
    fi
    kill "$pid"
    echo "服务已停止"
}

restart() {
    echo "重启服务..."
    echo "将先停止服务，然后启动服务"
    read -p "确认执行？(y/n): " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        echo "取消执行"
        return
    fi
    stop
    sleep 2
    start
}

# 交互式菜单
show_menu() {
    echo "================================"
    echo "Bitalos Agent 控制脚本"
    echo "================================"
    echo "1. 启动服务"
    echo "2. 停止服务"
    echo "3. 重启服务"
    echo "4. 退出"
    echo "================================"
    read -p "请选择操作 (1-4): " choice
}

# 主循环
while true; do
    # 显示配置文件列表
    echo "可用的配置文件:"
    if [ -d "$CONFIG_DIR" ]; then
        # 使用命令替换来处理通配符并忽略错误
        config_files=($(ls -d "$CONFIG_DIR"/*.json "$CONFIG_DIR"/*.yaml "$CONFIG_DIR"/*.yml "$CONFIG_DIR"/*.toml 2>/dev/null))
        if [ ${#config_files[@]} -eq 0 ]; then
            echo "错误: 配置目录中没有找到配置文件"
            exit 1
        fi
        
        for ((i=0; i<${#config_files[@]}; i++)); do
            config_file=$(basename "${config_files[$i]}")
            echo "  $((i+1)). $config_file"
        done
        
        read -p "请选择配置文件 (1-${#config_files[@]}): " config_choice
        if [[ "$config_choice" =~ ^[0-9]+$ ]] && [ "$config_choice" -ge 1 ] && [ "$config_choice" -le ${#config_files[@]} ]; then
            CONFIG_FILE_NAME=$(basename "${config_files[$((config_choice-1))]}")
            CONFIG="$CONFIG_DIR/$CONFIG_FILE_NAME"
            echo "已选择配置文件: $CONFIG_FILE_NAME"
        else
            echo "无效的选择"
            continue
        fi
    else
        echo "错误: 配置目录 $CONFIG_DIR 不存在"
        exit 1
    fi
    
    # 显示操作菜单
    show_menu
    
    case "$choice" in
        1)
            start
            ;;
        2)
            stop
            ;;
        3)
            restart
            ;;
        4)
            echo "退出脚本"
            exit 0
            ;;
        *)
            echo "无效的选择，请重新输入"
            ;;
    esac
    
    echo ""
    read -p "按回车键继续..." dummy
done