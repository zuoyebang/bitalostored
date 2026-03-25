#!/bin/bash

# 确保终端正确处理删除键
stty erase ^?

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INIT_SQL_FILE="${SCRIPT_DIR}/init.sql"

echo "=========================================="
echo "   MySQL 交互式初始化脚本"
echo "=========================================="
echo ""

read -p "请输入MySQL用户名 (示例: root): " MYSQL_USER

read -s -p "请输入MySQL密码 (示例: 123456): " MYSQL_PASSWORD
echo ""

read -p "请输入MySQL主机地址和端口 (示例: 127.0.0.1:3306): " MYSQL_HOSTPORT

MYSQL_HOST="${MYSQL_HOSTPORT%:*}"
MYSQL_PORT="${MYSQL_HOSTPORT#*:}"

echo ""
echo "=========================================="
echo "   连接信息确认"
echo "=========================================="
echo "用户名: ${MYSQL_USER}"
echo "主机: ${MYSQL_HOST}"
echo "端口: ${MYSQL_PORT}"
echo "=========================================="
echo ""

read -p "请输入数据库名称 (示例: bitalos): " MYSQL_DBNAME

echo ""
echo "=========================================="
echo "   测试数据库连接"
echo "=========================================="

if ! mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e "SELECT 1;"; then
    echo "错误: 无法连接到MySQL数据库，请检查连接信息"
    exit 1
fi

echo "数据库连接成功!"
echo ""

echo "=========================================="
echo "   检查/创建数据库"
echo "=========================================="

DB_EXISTS=$(mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e "SHOW DATABASES LIKE '${MYSQL_DBNAME}';" 2>/dev/null | grep -c "${MYSQL_DBNAME}")

if [ "${DB_EXISTS}" -gt 0 ]; then
    echo "数据库 ${MYSQL_DBNAME} 已存在，跳过创建"
else
    echo "数据库 ${MYSQL_DBNAME} 不存在，正在创建..."
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e "CREATE DATABASE \`${MYSQL_DBNAME}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    if [ $? -eq 0 ]; then
        echo "数据库 ${MYSQL_DBNAME} 创建成功"
    else
        echo "错误: 数据库创建失败"
        exit 1
    fi
fi

echo ""

echo "=========================================="
echo "   检查init.sql文件"
echo "=========================================="

if [ ! -f "${INIT_SQL_FILE}" ]; then
    echo "错误: init.sql文件不存在于 ${INIT_SQL_FILE}"
    exit 1
fi

echo "init.sql文件存在: ${INIT_SQL_FILE}"
echo ""

echo "=========================================="
echo "   执行init.sql创建表"
echo "=========================================="

mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" < "${INIT_SQL_FILE}"

if [ $? -eq 0 ]; then
    echo "表创建成功"
else
    echo "错误: 表创建失败"
    exit 1
fi

echo ""

echo "=========================================="
echo "   初始化Dashboard账号信息"
echo "=========================================="

read -p "请输入Dashboard用户名 (示例: admin): " DASHBOARD_USERNAME

read -s -p "请输入Dashboard密码 (示例: admin123): " DASHBOARD_PASSWORD
echo ""

read -p "请输入PaaS用户名 (示例: paas): " PAAS_USERNAME

read -s -p "请输入PaaS密码 (示例: paas123): " PAAS_PASSWORD
echo ""

echo ""
echo "=========================================="
echo "   插入Dashboard数据到tblDashboard"
echo "=========================================="

# 构建JSON字符串变量
DASHBOARD_JSON="{\"username\":\"${DASHBOARD_USERNAME}\",\"password\":\"${DASHBOARD_PASSWORD}\",\"role\":1}"
PAAS_JSON="{\"username\":\"${PAAS_USERNAME}\",\"password\":\"${PAAS_PASSWORD}\",\"role\":1}"

mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" -e "
    INSERT INTO tblDashboard (product_name, sub_path, full_path, value, create_time, update_time)
    VALUES ('admin', '${DASHBOARD_USERNAME}', '/stored/admin/${DASHBOARD_USERNAME}', '${DASHBOARD_JSON}', UNIX_TIMESTAMP(), 0),
           ('paasadmin', '${PAAS_USERNAME}', '/stored/paasadmin/${PAAS_USERNAME}', '${PAAS_JSON}', UNIX_TIMESTAMP(), 0)
    ON DUPLICATE KEY UPDATE value = VALUES(value), update_time = UNIX_TIMESTAMP();
"

if [ $? -eq 0 ]; then
    echo "Dashboard数据插入成功"
else
    echo "错误: Dashboard数据插入失败"
    exit 1
fi

echo ""

echo "=========================================="
echo "   插入配置到tblConfig"
echo "=========================================="

insertDashboardConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/dh.toml")
    # 替换数据库配置
    content=$(echo "${content}" | sed "s/username = .*/username = \"${MYSQL_USER}\"/g")
    content=$(echo "${content}" | sed "s/password = .*/password = \"${MYSQL_PASSWORD}\"/g")
    content=$(echo "${content}" | sed "s/hostport = .*/hostport = \"${MYSQL_HOSTPORT}\"/g")
    content=$(echo "${content}" | sed "s/dbname = .*/dbname = \"${MYSQL_DBNAME}\"/g")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('config/dashboard.toml', 1, 'config', '0644', 'default', 1, FROM_BASE64('${encoded_content}'), 3, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertDhShConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/dh.sh")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('run.sh', 1, 'config', '0755', 'default', 1, FROM_BASE64('${encoded_content}'), 3, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertFeConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/fe.toml")
    # 替换数据库配置
    content=$(echo "${content}" | sed "s/username = .*/username = \"${MYSQL_USER}\"/g")
    content=$(echo "${content}" | sed "s/password = .*/password = \"${MYSQL_PASSWORD}\"/g")
    content=$(echo "${content}" | sed "s/hostport = .*/hostport = \"${MYSQL_HOSTPORT}\"/g")
    content=$(echo "${content}" | sed "s/dbname = .*/dbname = \"${MYSQL_DBNAME}\"/g")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('config/fe.toml', 1, 'config', '0755', 'default', 1, FROM_BASE64('${encoded_content}'), 4, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertFeShConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/fe.sh")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('run.sh', 1, 'config', '0755', 'default', 1, FROM_BASE64('${encoded_content}'), 4, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertProxyConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/proxy.toml")
    content=$(echo "${content}" | sed "s/dashboard_username = .*/dashboard_username = \"${DASHBOARD_USERNAME}\"/g")
    content=$(echo "${content}" | sed "s/dashboard_password = .*/dashboard_password = \"${DASHBOARD_PASSWORD}\"/g")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('config/proxy-txcloud.toml', 1, 'config', '0644', 'default', 1, FROM_BASE64('${encoded_content}'), 2, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertProxyShConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/proxy.sh")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('run.sh', 1, 'config', '0755', 'default', 1, FROM_BASE64('${encoded_content}'), 2, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertProxySuperConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/proxy_supervisor.conf")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('config/supervisor.conf', 1, 'config', '0644', 'default', 1, FROM_BASE64('${encoded_content}'), 2, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertStoredConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/stored_config.toml")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('config/config.toml', 1, 'config', '0644', 'default', 1, FROM_BASE64('${encoded_content}'), 6, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertStoredShConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/stored.sh")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('run.sh', 1, 'config', '0644', 'default', 1, FROM_BASE64('${encoded_content}'), 6, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

insertStoredSupConfig() {
    local content=$(cat "${SCRIPT_DIR}/../conf/stored_supervisor.conf")

    # 使用base64编码避免引号问题
    local encoded_content=$(echo "${content}" | base64)

    # 使用标准输入传递SQL语句
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" <<EOF
        INSERT INTO tblConfig (name, need_render, file_type, file_mode, config_pack_name, config_pack_id, content, service_id, create_time)
        VALUES ('config/supervisor.conf', 1, 'config', '0644', 'default', 1, FROM_BASE64('${encoded_content}'), 6, UNIX_TIMESTAMP())
        ON DUPLICATE KEY UPDATE content = VALUES(content), update_time = UNIX_TIMESTAMP();
EOF
}

echo "=========================================="
echo "   初始化tblService表"
echo "=========================================="

mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DBNAME}" -e "
    INSERT INTO tblService (name, port_range, cluster_port_range, create_time, update_time) VALUES
    ('stored-matrix', '[]', '[]', UNIX_TIMESTAMP(), 0),
    ('stored-proxy', '[]', '[]', UNIX_TIMESTAMP(), 0),
    ('stored-dashboard', '[]', '[]', UNIX_TIMESTAMP(), 0),
    ('stored-fe', '[]', '[]', UNIX_TIMESTAMP(), 0),
    ('stored-agent', '[]', '[]', UNIX_TIMESTAMP(), 0),
    ('stored-bitalos', '[10000,19999]', '[20000, 29999]', UNIX_TIMESTAMP(), 0)
    ON DUPLICATE KEY UPDATE update_time = UNIX_TIMESTAMP();
"

if [ $? -eq 0 ]; then
    echo "tblService表初始化成功"
else
    echo "错误: tblService表初始化失败"
    exit 1
fi

echo "插入dashboard配置..."
insertDashboardConfig

echo "插入dashboard shell脚本..."
insertDhShConfig

echo "插入FE配置..."
insertFeConfig

echo "插入FE shell脚本..."
insertFeShConfig

echo "插入proxy配置..."
insertProxyConfig

echo "插入proxy shell脚本..."
insertProxyShConfig

echo "插入proxy supervisor脚本..."
insertProxySuperConfig

echo "插入stored配置..."
insertStoredConfig

echo "插入stored shell脚本..."
insertStoredShConfig

echo "插入stored supervisor脚本..."
insertStoredSupConfig

echo "配置插入成功"
echo ""

echo "=========================================="
echo "   MYSQL初始化完成"
echo "=========================================="
echo "数据库名称: ${MYSQL_DBNAME}"
echo "Dashboard用户名: ${DASHBOARD_USERNAME}"
echo "PaaS用户名: ${PAAS_USERNAME}"
echo ""
echo "MYSQL初始化步骤已完成!"
echo "=========================================="
