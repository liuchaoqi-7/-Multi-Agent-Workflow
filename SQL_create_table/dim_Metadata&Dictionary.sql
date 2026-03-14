# 元数据管理与数据字典 (Metadata & Dictionary)
----
    CREATE TABLE dim.dim_sys_metadata_dict (
    
        `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
        `db_name` varchar(64) NOT NULL COMMENT '库名 (ODS/DWD/DIM/ADS)',
        `table_name` varchar(128) NOT NULL COMMENT '表英文名',
        `table_comment` varchar(256) DEFAULT NULL COMMENT '表中文注释/业务含义',
        `column_name` varchar(128) NOT NULL COMMENT '字段英文名',
        `column_type` varchar(64) DEFAULT NULL COMMENT '字段完整类型 (如 decimal(18,2))',
        `column_comment` text COMMENT '字段业务口径/逻辑说明',
        `is_pk` tinyint(1) DEFAULT '0' COMMENT '是否主键 (1=是, 0=否)',
        `is_nullable` varchar(10) DEFAULT 'YES' COMMENT '是否允许为空',
        `column_order` int(11) DEFAULT '0' COMMENT '字段排序',
        
        -- 🌟 新增：企业级管理字段
        `data_owner` varchar(64) DEFAULT 'DataTeam' COMMENT '数据负责人',
        `security_level` varchar(20) DEFAULT 'L1' COMMENT '安全等级 (L1:公开, L2:内部, L3:绝密)',
        `tag_list` varchar(255) DEFAULT NULL COMMENT '标签 (如: 财务, 流量, 核心)',
        
        `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '首次同步时间',
        `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
        
        PRIMARY KEY (`id`),
        -- 核心逻辑：确保同一个库、同一个表、同一个字段只有一行记录
        UNIQUE KEY `uk_db_table_col` (`db_name`,`table_name`,`column_name`),
        KEY `idx_table` (`table_name`),
        KEY `idx_comment` (`column_comment`(100))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='[核心] 全域数据字典与元数据表';


INSERT INTO dim.dim_sys_metadata_dict (
    db_name, 
    table_name, 
    table_comment,    -- 新增：同步表的注释
    column_name, 
    column_type, 
    column_comment, 
    is_pk,            -- 新增：自动判断主键
    is_nullable, 
    column_order
)
SELECT 
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    t.TABLE_COMMENT,  -- 从 TABLES 表获取表的注释
    c.COLUMN_NAME,
    -- 智能拼接类型，保留精度信息
    CASE 
        WHEN c.DATA_TYPE IN ('varchar', 'char') THEN CONCAT(c.DATA_TYPE, '(', c.CHARACTER_MAXIMUM_LENGTH, ')')
        WHEN c.DATA_TYPE IN ('decimal', 'float', 'double') AND c.NUMERIC_PRECISION IS NOT NULL 
            THEN CONCAT(c.DATA_TYPE, '(', c.NUMERIC_PRECISION, ',', c.NUMERIC_SCALE, ')')
        ELSE c.DATA_TYPE 
    END AS column_type,
    c.COLUMN_COMMENT,
    CASE WHEN c.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END AS is_pk,
    c.IS_NULLABLE,
    c.ORDINAL_POSITION
FROM information_schema.COLUMNS c
LEFT JOIN information_schema.TABLES t 
    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA 
    AND c.TABLE_NAME = t.TABLE_NAME
WHERE 
    -- 🎯 圈定你的业务库，排除系统库
    c.TABLE_SCHEMA IN ('ods', 'dwd', 'dim', 'ads', 'bi_report')
    AND c.TABLE_NAME NOT LIKE '%_bak%'  -- 排除备份表
    AND c.TABLE_NAME NOT LIKE '%_tmp%'  -- 排除临时表
ORDER BY c.TABLE_SCHEMA
ON DUPLICATE KEY UPDATE
    -- 当字段已存在时，更新技术属性，但保留你手动维护的业务属性（如 data_owner）
    table_comment = VALUES(table_comment),
    column_type = VALUES(column_type),
    column_comment = VALUES(column_comment), -- 如果源库修改了注释，这里会同步
    is_pk = VALUES(is_pk),
    is_nullable = VALUES(is_nullable),
    column_order = VALUES(column_order),
    update_time = NOW();

