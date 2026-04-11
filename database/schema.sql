-- 沪深300ETF数据库表结构
-- 数据库名: etf_data

-- 创建数据库（如不存在）
CREATE DATABASE IF NOT EXISTS etf_data DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE etf_data;

-- ============================================
-- 1. ETF基础信息表
-- ============================================
CREATE TABLE IF NOT EXISTS etf_info (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    sec_code VARCHAR(10) NOT NULL UNIQUE COMMENT '证券代码',
    sec_name VARCHAR(50) NOT NULL COMMENT '证券简称',
    fund_name VARCHAR(100) NOT NULL COMMENT '基金全称',
    etf_type VARCHAR(20) DEFAULT '跨市' COMMENT 'ETF类型(单市/跨市/其他)',
    listing_date DATE COMMENT '上市日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_sec_code (sec_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF基础信息表';

-- ============================================
-- 2. ETF每日份额数据表
-- ============================================
CREATE TABLE IF NOT EXISTS etf_daily_share (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    stat_date DATE NOT NULL COMMENT '统计日期',
    sec_code VARCHAR(10) NOT NULL COMMENT '证券代码',
    tot_vol DECIMAL(20,2) NOT NULL COMMENT '总份额(万份)',
    num INT COMMENT '排名序号',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_date_code (stat_date, sec_code) COMMENT '日期+代码唯一索引',
    INDEX idx_stat_date (stat_date),
    INDEX idx_sec_code (sec_code),
    FOREIGN KEY (sec_code) REFERENCES etf_info(sec_code) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF每日份额数据表';

-- ============================================
-- 3. ETF每日汇总表
-- ============================================
CREATE TABLE IF NOT EXISTS etf_daily_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    stat_date DATE NOT NULL UNIQUE COMMENT '统计日期',
    total_vol DECIMAL(20,2) NOT NULL COMMENT '当日总份额(万份)',
    etf_count INT NOT NULL COMMENT 'ETF数量',
    avg_vol DECIMAL(20,2) COMMENT '平均份额(万份)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_stat_date (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF每日汇总表';

-- ============================================
-- 4. 指数每日汇总表（上证指数）
-- ============================================
CREATE TABLE IF NOT EXISTS index_daily_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stat_date DATE NOT NULL UNIQUE COMMENT '统计日期',
    index_code VARCHAR(10) NOT NULL DEFAULT '000001' COMMENT '指数代码',
    index_name VARCHAR(50) NOT NULL DEFAULT '上证指数' COMMENT '指数名称',
    total_amount DECIMAL(20,2) NOT NULL COMMENT '成交额(万元)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stat_date (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数每日汇总表';

-- ============================================
-- 5. 数据采集日志表
-- ============================================
CREATE TABLE IF NOT EXISTS etf_fetch_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    fetch_date DATE NOT NULL COMMENT '采集日期',
    status VARCHAR(20) NOT NULL COMMENT '状态(success/failed)',
    records_count INT DEFAULT 0 COMMENT '采集记录数',
    error_msg TEXT COMMENT '错误信息',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_fetch_date (fetch_date),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据采集日志表';

-- ============================================
-- 初始数据：ETF基础信息
-- ============================================
INSERT IGNORE INTO etf_info (sec_code, sec_name, fund_name) VALUES
('510300', '300ETF', '沪深300ETF华泰柏瑞'),
('510310', 'HS300ETF', '沪深300ETF易方达'),
('510320', '沪深300ETF', '沪深300ETF中金'),
('510330', '沪深300ETF', '沪深300ETF华夏'),
('510350', '沪深300ETF', '沪深300ETF工银'),
('510360', '沪深300ETF', '沪深300ETF广发');
