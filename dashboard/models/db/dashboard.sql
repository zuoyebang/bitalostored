CREATE TABLE `tblDashboard` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `product_name` varchar(512) NOT NULL DEFAULT '',
    `sub_path` varchar(512) NOT NULL DEFAULT '',
    `full_path` varchar(512) NOT NULL DEFAULT '',
    `value` text,
    `create_time` int unsigned NOT NULL DEFAULT '0',
    `update_time` int unsigned NOT NULL DEFAULT '0'
);