import pandas as pd
import mysql.connector
from datetime import datetime
import os
import sys

# Tên các bảng
FACT_PRICE_TABLE = "FACT_GOLD_PRICE"
DIM_BRAND_TABLE = "DIM_GOLD_TYPE"
AGGREGATE_TABLE_NAME = "AGG_DAILY_GOLD_SUMMARY" 

def connect_db(db_name):
    """Thiết lập kết nối tới MySQL Database (sử dụng host Docker chung)."""
    DB_HOST = "mysql-staging-db" 
    DB_USER = os.environ.get("MYSQL_USER", "root")
    DB_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root_password_local")

    return mysql.connector.connect(
        host=DB_HOST,
        database=db_name,
        user=DB_USER,
        password=DB_PASSWORD
    )

def run_aggregate_data():
    """Thực hiện tổng hợp dữ liệu (ví dụ: tính giá trung bình hàng ngày theo thương hiệu và khu vực)."""
    
    try:
        # 1. KẾT NỐI VÀ TRUY VẤN DỮ LIỆU TỪ FACT TABLE
        conn = connect_db("db_warehouse") 
        cursor = conn.cursor()
        
        aggregation_query = f"""
        SELECT 
            t1.gold_type_id,
            t1.gold_region_id, 
            t2.brand_name,
            DATE(t1.capture_timestamp) AS capture_date,
            AVG(t1.buy_price) AS avg_buy_price,
            MAX(t1.sell_price) AS max_sell_price,
            MIN(t1.sell_price) AS min_sell_price,
            AVG(t1.spread) AS avg_spread
        FROM {FACT_PRICE_TABLE} t1
        JOIN {DIM_BRAND_TABLE} t2 ON t1.gold_type_id = t2.id
        GROUP BY 
            t1.gold_type_id, 
            t1.gold_region_id, 
            t2.brand_name,
            capture_date
        ORDER BY 
            capture_date DESC;
        """

        df_agg = pd.read_sql(aggregation_query, conn)
        
        # 2. TẠO BẢNG TỔNG HỢP (AGGREGATE) TRONG DB WAREHOUSE (Nếu chưa có)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {AGGREGATE_TABLE_NAME} (
            gold_type_id INT,
            gold_region_id INT, 
            brand_name VARCHAR(255),
            capture_date DATE,
            avg_buy_price DECIMAL(20, 0),
            max_sell_price DECIMAL(20, 0),
            min_sell_price DECIMAL(20, 0),
            avg_spread DECIMAL(20, 0),
            PRIMARY KEY (gold_type_id, gold_region_id, capture_date) 
        );
        """
        cursor.execute(create_table_query)

        # 3. GHI DỮ LIỆU ĐÃ TỔNG HỢP VÀO BẢNG AGGREGATE
        
        today_date_str = datetime.now().strftime("%Y-%m-%d")
        
        delete_query = f"DELETE FROM {AGGREGATE_TABLE_NAME} WHERE capture_date = '{today_date_str}'"
        cursor.execute(delete_query)

        insert_query = f"""
        INSERT INTO {AGGREGATE_TABLE_NAME} 
        (gold_type_id, gold_region_id, brand_name, capture_date, avg_buy_price, max_sell_price, min_sell_price, avg_spread)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        df_today = df_agg[df_agg['capture_date'].astype(str) == today_date_str]
        
        for index, row in df_today.iterrows():
            cursor.execute(insert_query, (
                row['gold_type_id'],
                row['gold_region_id'], # <--- ĐÃ SỬA LỖI CÚ PHÁP Ở ĐÂY
                row['brand_name'],
                row['capture_date'],
                round(row['avg_buy_price']),
                row['max_sell_price'],
                row['min_sell_price'],
                round(row['avg_spread'])
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Đã tổng hợp thành công {len(df_today)} bản ghi và Load vào {AGGREGATE_TABLE_NAME}.")
        return True

    except Exception as e:
        print(f"LỖI KHI TỔNG HỢP DỮ LIỆU (AGGREGATION): {e}")
        return False

if __name__ == "__main__":
    if run_aggregate_data():
        sys.exit(0)
    else:
        sys.exit(1)