-- *************************************************************
-- 1. SCHEMA CHO DB_STAGING (Sử dụng bởi load_staging.py)
-- *************************************************************
CREATE DATABASE IF NOT EXISTS db_staging;
USE db_staging;

-- Bảng gold_raw (nhận data thô từ extract_data.py)
CREATE TABLE IF NOT EXISTS gold_raw (
    raw_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    region TEXT,
    gold_type TEXT,
    gold_item TEXT,
    buy_raw TEXT,
    sell_raw TEXT,
    updated_time DATETIME, 
    source_url TEXT
);

-- *************************************************************
-- 2. SCHEMA CHO DB_WAREHOUSE (Sử dụng bởi load_datawarehouse.py)
-- *************************************************************
CREATE DATABASE IF NOT EXISTS db_warehouse;
USE db_warehouse;

-- Bảng Dimension (Chiều) MỚI: DIM_REGION
CREATE TABLE IF NOT EXISTS DIM_REGION (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL UNIQUE
);

-- Bảng Dimension (Chiều) CŨ: DIM_GOLD_TYPE
CREATE TABLE IF NOT EXISTS DIM_GOLD_TYPE (
    id INT AUTO_INCREMENT PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL UNIQUE
);

-- Bảng Fact (Sự kiện) CẬP NHẬT: Thêm gold_region_id
CREATE TABLE IF NOT EXISTS FACT_GOLD_PRICE (
    id INT AUTO_INCREMENT PRIMARY KEY,
    gold_type_id INT NOT NULL,
    gold_region_id INT NOT NULL,
    buy_price INT NOT NULL,
    sell_price INT NOT NULL,
    spread INT,
    capture_timestamp DATETIME NOT NULL,
    FOREIGN KEY (gold_type_id) REFERENCES DIM_GOLD_TYPE(id),
    FOREIGN KEY (gold_region_id) REFERENCES DIM_REGION(id)
);

-- Bảng Aggregate (CẬP NHẬT)
CREATE TABLE IF NOT EXISTS AGG_DAILY_GOLD_SUMMARY (
    gold_type_id INT,
    gold_region_id INT,
    brand_name VARCHAR(255),
    region_name VARCHAR(255),
    capture_date DATE,
    avg_buy_price DECIMAL(10, 0),
    max_sell_price DECIMAL(10, 0),
    min_sell_price DECIMAL(10, 0),
    avg_spread DECIMAL(10, 0),
    PRIMARY KEY (gold_type_id, gold_region_id, capture_date)
);

-- *************************************************************
-- 3. SCHEMA CHO DB_DATAMART (Sử dụng bởi load_datamart.py)
-- *************************************************************
CREATE DATABASE IF NOT EXISTS db_datamart;
USE db_datamart;

-- Tạo bảng Data Mart cho phân tích giá/khu vực
CREATE TABLE IF NOT EXISTS DM_REGION_SUMMARY (
    region_name VARCHAR(255) PRIMARY KEY,
    avg_spread_hcm DECIMAL(10, 0),
    avg_spread_mienbac DECIMAL(10, 0),
    last_update DATETIME
);
