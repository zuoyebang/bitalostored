CREATE TABLE IF NOT EXISTS `tblCluster` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id集群clusterId',
    `name` varchar(512) NOT NULL DEFAULT '' COMMENT '集群cluster名称',
    `status` varchar(45) NOT NULL DEFAULT '' COMMENT '集群cluster状态',
    `region_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '集群所属的region',
    `stored_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '一个stored ID 表示一套dashboard fe proxy bitalos ',
    `service_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '集群部署的服务id',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    `monitor` varchar(255) DEFAULT NULL COMMENT 'monitor address',
    `config_pack_id` int(10) unsigned NOT NULL COMMENT '集群绑定的配置文件',
    `auth` varchar(512) NOT NULL DEFAULT '' COMMENT '集群配置的auth',
    `deraft_token` varchar(512) NOT NULL DEFAULT '' COMMENT 'matrix集群配置的deraft token',
    `department` varchar(45) DEFAULT '' COMMENT '集群所属部门',
    `cluster_group` varchar(30) NOT NULL DEFAULT '' COMMENT '集群组',
    `is_stored1` tinyint(1) DEFAULT NULL COMMENT '是否stored1.0',
    PRIMARY KEY (`id`),
    KEY `region_id` (`region_id`),
    KEY `service_id` (`service_id`),
    KEY `config_pack_id` (`config_pack_id`),
    KEY `department` (`department`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cluster信息表';

CREATE TABLE IF NOT EXISTS `tblConfig` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id',
    `name` varchar(512) NOT NULL DEFAULT '' COMMENT '配置的名称',
    `need_render` tinyint(1) DEFAULT NULL COMMENT '是否需要渲染',
    `idc` varchar(45) DEFAULT '' COMMENT '配置的云属性，支持不同的云使用不同的配置，为空表示不考虑云属性',
    `file_type` varchar(45) DEFAULT '' COMMENT '文件类型',
    `file_mode` varchar(45) DEFAULT '' COMMENT '文件权限',
    `config_pack_name` varchar(512) NOT NULL DEFAULT '' COMMENT '跟cluster名字一样',
    `config_pack_id` int(10) unsigned NOT NULL COMMENT '一个cluster对应一个config_pack_id',
    `cluster_id` int(10) unsigned DEFAULT '0' COMMENT '绑定的cluster_id',
    `content` text COMMENT '配置的详细内容',
    `comment` text COMMENT '配置文件改动的注解',
    `last_version` text COMMENT '上一个版本配置的详细内容',
    `service_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '配置所属的服务id',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idc` (`idc`),
    KEY `service_id` (`service_id`),
    KEY `cluster_id` (`cluster_id`),
    KEY `config_pack_id` (`config_pack_id`),
    KEY `need_render` (`need_render`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cluster配置表';

CREATE TABLE IF NOT EXISTS `tblCosFile` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id集群clusterId',
    `name` varchar(512) NOT NULL DEFAULT '' COMMENT '文件的名称',
    `cos_key` varchar(512) NOT NULL DEFAULT '' COMMENT 'cos文件的key',
    `file_type` varchar(45) DEFAULT '' COMMENT '文件类型',
    `file_mode` varchar(45) DEFAULT '' COMMENT '文件权限',
    `hash` varchar(45) NOT NULL DEFAULT '' COMMENT '配置的云属性，支持不同的云使用不同的配置',
    `version` varchar(45) NOT NULL DEFAULT '' COMMENT '文件的版本，对应git tag',
    `service_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '配置所属的服务id',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `version` (`version`),
    KEY `service_id` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='服务对应二进制文件表';

CREATE TABLE IF NOT EXISTS `tblDashboard` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `product_name` varchar(512) NOT NULL DEFAULT '',
    `sub_path` varchar(512) NOT NULL DEFAULT '' COMMENT 'dashboard',
    `full_path` varchar(512) NOT NULL DEFAULT '' COMMENT 'dashboard',
    `value` text COMMENT 'full_path',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    UNIQUE KEY `full_path` (`full_path`),
    KEY `product_name` (`product_name`),
    KEY `sub_path` (`sub_path`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dashboard';

CREATE TABLE IF NOT EXISTS `tblGroup` (
    `group_id` int(10) unsigned NOT NULL COMMENT '主键id分片id',
    `status` varchar(45) NOT NULL DEFAULT '' COMMENT '分片状态',
    `cluster_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '分片所属的集群',
    `service_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'service id',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    `init_raft` varchar(512) NOT NULL DEFAULT '' COMMENT 'matrix集群初始节点信息',
    `locked` tinyint(1) DEFAULT NULL COMMENT '分片是否锁定',
    `init_node_id` varchar(512) NOT NULL DEFAULT '' COMMENT '集群初始nodeId',
    `max_node_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '最大nodeId',
    PRIMARY KEY (`group_id`,`cluster_id`),
    KEY `cluster_id` (`cluster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='集群分片信息表';

CREATE TABLE IF NOT EXISTS `tblHostPort` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id',
    `machine_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '机器id',
    `port` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '端口',
    `ip` varchar(45) NOT NULL DEFAULT '' COMMENT '机器ip地址',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `machine_port` (`machine_id`,`port`,`ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='端口资源管理表';

CREATE TABLE IF NOT EXISTS `tblMachine` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id机器id',
    `status` varchar(45) NOT NULL DEFAULT '' COMMENT '机器状态',
    `weight` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '计算机器部署实例数量时的权重',
    `idc` varchar(45) NOT NULL DEFAULT '' COMMENT '机器所在的机房',
    `ip` varchar(45) NOT NULL DEFAULT '' COMMENT '机器绑定的ip地址',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    `budget` varchar(45) NOT NULL DEFAULT '' COMMENT '机器预算单元',
    `host_name` varchar(512) NOT NULL DEFAULT '' COMMENT 'hostname',
    `need_upgrade` varchar(45) NOT NULL DEFAULT '' COMMENT 'agent是否有升级任务',
    `upgrade_version` int(10) unsigned NOT NULL COMMENT 'agent需要升级到的版本',
    `upgrade_config` text COMMENT 'agent升级的配置',
    `agent_config` text COMMENT 'agent配置的详细内容',
    `version` varchar(45) NOT NULL DEFAULT '' COMMENT 'agent当前版本',
    `cpu_set` varchar(5000) NOT NULL DEFAULT '' COMMENT '集群和对应cpu编号',
    `cpu_set_max` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'cpu编号最大值',
    `cpu_total` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT 'cpu总数',
    `share_cpu_set` varchar(5000) NOT NULL DEFAULT '' COMMENT '共享cpu的集群',
    `is_virtual` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '虚拟机',
    PRIMARY KEY (`id`),
    KEY `status` (`status`),
    KEY `budget` (`budget`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='机器列表';

CREATE TABLE IF NOT EXISTS `tblNode` (
    `node_id` int(10) unsigned NOT NULL COMMENT '实例id',
    `status` varchar(45) NOT NULL DEFAULT '' COMMENT '实例node状态',
    `cluster_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例所属的集群',
    `group_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例所属的分片',
    `region_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例所在的region',
    `machine_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例所在的机器',
    `service_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例的服务类型',
    `package_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '部署实例使用的部署包id',
    `service_port` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例监听的端口',
    `cluster_port` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '实例之间的通讯端口',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    `cos_file_id` int(10) unsigned NOT NULL COMMENT '节点二进制文件版本',
    `cos_file_version` varchar(45) NOT NULL DEFAULT '' COMMENT '二进制文件的版本，对应git tag',
    `config_content` text COMMENT '配置的详细内容',
    `cpu_throttled_nr` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'cpu受限次数',
    `is_witness` tinyint(1) DEFAULT NULL COMMENT '是否为witness节点',
    PRIMARY KEY (`node_id`,`group_id`,`cluster_id`),
    KEY `cluster_id` (`cluster_id`),
    KEY `group_id` (`group_id`),
    KEY `machine_id` (`machine_id`),
    KEY `service_id` (`service_id`),
    KEY `region_id` (`region_id`),
    KEY `status` (`status`),
    KEY `cos_file_id` (`cos_file_id`),
    KEY `is_witness` (`is_witness`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='集群部署实例信息表';

CREATE TABLE IF NOT EXISTS `tblOperationRecord` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id',
    `url` varchar(512) NOT NULL DEFAULT '' COMMENT '操作的url',
    `cookie` varchar(512) NOT NULL DEFAULT '' COMMENT 'operator的cookie',
    `uid` varchar(512) DEFAULT '' COMMENT 'operator的uid',
    `module` varchar(45) NOT NULL DEFAULT '' COMMENT '产生operation的模块',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `url` (`url`),
    KEY `uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='操作记录表';

CREATE TABLE IF NOT EXISTS `tblOpsActionLog` (
    `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `ip` varchar(45) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT 'ip',
    `port` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'port',
    `cluster_name` varchar(512) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT 'name',
    `action_type` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'type',
    `op_name` varchar(45) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT 'op name',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'time',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='运维事件记录表';

CREATE TABLE IF NOT EXISTS `tblRegion` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id区域id',
    `new_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'new regionid',
    `name` varchar(512) NOT NULL DEFAULT '' COMMENT '区域名字',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='区域列表';

CREATE TABLE IF NOT EXISTS `tblRegionMachine` (
    `region_id` int(10) unsigned NOT NULL COMMENT 'region id',
    `machine_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'machine id',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`region_id`,`machine_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='region machine 关系表';

CREATE TABLE IF NOT EXISTS `tblResourcePool` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `cluster_name` varchar(50) NOT NULL DEFAULT '' COMMENT '集群名称',
    `cluster_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '集群ID',
    `service_id` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '服务ID',
    `idc` varchar(50) NOT NULL DEFAULT '' COMMENT 'idc',
    `metric_name` varchar(20) NOT NULL DEFAULT '' COMMENT '指标名称',
    `cgroup_limit` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '线上实际限制',
    `suggest_value` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '系统建议值',
    `manual_value` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '人工设置值',
    `cost_value` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '成本',
    `sync_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '从过程表同步时间',
    `apply_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '应用时间',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '记录创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '记录更改时间',
    `cpu_set_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'cpu绑定类型',
    `port` int(11) NOT NULL DEFAULT '0' COMMENT '端口',
    `min_cpu` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '最小cpu编号',
    `max_cpu` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '最大cpu编号',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资源池结果表';

CREATE TABLE IF NOT EXISTS `tblService` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id服务类型id',
    `name` varchar(512) NOT NULL DEFAULT '' COMMENT '服务类型名字',
    `port_range` varchar(512) NOT NULL DEFAULT '' COMMENT '服务监听的端口所在区间',
    `cluster_port_range` varchar(512) NOT NULL DEFAULT '' COMMENT '服务间通讯端口所在区间',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='服务类型列表';

CREATE TABLE IF NOT EXISTS `tblSlotAction` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary',
    `cluster_name` varchar(512) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT 'cluster name',
    `action` varchar(512) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT 'action: remove/migrate',
    `slot_start` int(11) NOT NULL DEFAULT '0' COMMENT 'slot start',
    `slot_end` int(11) NOT NULL DEFAULT '0' COMMENT 'slot end',
    `src_group` int(11) NOT NULL DEFAULT '0' COMMENT 'src group',
    `dst_group` int(11) NOT NULL DEFAULT '0' COMMENT 'dst group',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'start time',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'update time',
    PRIMARY KEY (`id`),
    KEY `idx_cluster` (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='slot_action';

CREATE TABLE IF NOT EXISTS `tblTask` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id任务id',
    `status` varchar(45) NOT NULL DEFAULT '' COMMENT '任务状态',
    `type` varchar(45) NOT NULL DEFAULT '' COMMENT '任务类型',
    `cluster_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '任务所属的集群',
    `group_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '任务所属的分片',
    `node_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '任务所属的实例',
    `region_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '任务所在的region',
    `machine_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '任务所在的机器',
    `service_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '任务需要部署的服务类型',
    `package_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '执行任务使用的部署包id',
    `extra` text COMMENT '任务扩展字段，存放任务非公共信息',
    `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
    `cos_file_id` int(10) unsigned NOT NULL COMMENT '节点二进制文件版本',
    `cos_file_version` varchar(45) NOT NULL DEFAULT '' COMMENT 'version',
    PRIMARY KEY (`id`),
    KEY `cluster_id` (`cluster_id`),
    KEY `group_id` (`group_id`),
    KEY `machine_id` (`machine_id`),
    KEY `service_id` (`service_id`),
    KEY `region_id` (`region_id`),
    KEY `status` (`status`),
    KEY `type` (`type`),
    KEY `cos_file_id` (`cos_file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务列表';

CREATE TABLE `tblLock` (
   `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id',
   `lock_name` varchar(512) NOT NULL DEFAULT '' COMMENT '锁的名字',
   `create_time` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
   PRIMARY KEY (`id`),
   UNIQUE KEY `lock_name` (`lock_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='分布式锁' ;


