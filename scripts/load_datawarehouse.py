import pandas as pd
import mysql.connector
from datetime import datetime
import os
import sys

# Tên các bảng trong Data Warehouse DB (db_warehouse)
DIM_BRAND_TABLE = "DIM_GOLD_TYPE"
DIM_REGION_TABLE = "DIM_REGION" # <--- THÊM
FACT_PRICE_TABLE = "FACT_GOLD_PRICE"

def connect_db(db_name="db_warehouse"):
    """Thiết lập kết nối tới MySQL Data Warehouse DB."""
    # SỬA LỖI KẾT NỐI: Sử dụng tên service Docker chính xác
    DB_HOST = "mysql-staging-db" 
    DB_USER = os.environ.get("MYSQL_USER", "root")
    DB_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root_password_local")
    
    return mysql.connector.connect(
        host=DB_HOST,
        database=db_name,
        user=DB_USER,
        password=DB_PASSWORD
    )

def run_load_datawarehouse(base_dir):
    """Đọc dữ liệu sạch, phân tách thành Dimension/Fact và Load vào DB Warehouse."""
    
    # 1. TÌM FILE INPUT (File đã được Transform)
    today_str = datetime.now().strftime("%d%m%Y")
    file_name = f"giavang_cleaned_{today_str}.csv"
    input_path = os.path.join(base_dir, 'data', 'cleaned', file_name) 

    if not os.path.exists(input_path):
        print(f"LỖI LOAD DW: Không tìm thấy file input sạch: {input_path}")
        return False

    try:
        # 2. Đọc dữ liệu sạch từ file CSV
        df = pd.read_csv(input_path)
        
        # 3. KẾT NỐI DB VÀ CHUẨN BỊ
        conn = connect_db("db_warehouse")
        cursor = conn.cursor()

        # --- A. XỬ LÝ DIMENSION: DIM_GOLD_TYPE (Thương hiệu) ---
        brands = df[['brand_name']].drop_duplicates()

        brand_insert_query = f"""
        INSERT INTO {DIM_BRAND_TABLE} (brand_name)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE brand_name=brand_name;
        """
        for index, row in brands.iterrows():
            cursor.execute(brand_insert_query, (row['brand_name'],))
        
        conn.commit()
        
        # Lấy lại các khóa (Keys) của Dimension vừa chèn
        brand_map_query = f"SELECT brand_name, id FROM {DIM_BRAND_TABLE}"
        brand_map_df = pd.read_sql(brand_map_query, conn)
        brand_map = brand_map_df.set_index('brand_name')['id'].to_dict()
        df['gold_type_id'] = df['brand_name'].map(brand_map)

        # --- B. XỬ LÝ DIMENSION: DIM_REGION (Khu vực) ---
        regions = df[['region_name']].drop_duplicates()

        region_insert_query = f"""
        INSERT INTO {DIM_REGION_TABLE} (region_name)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE region_name=region_name;
        """
        for index, row in regions.iterrows():
            cursor.execute(region_insert_query, (row['region_name'],))

        conn.commit()
        
        # Lấy lại các khóa (Keys) của DIM_REGION
        region_map_query = f"SELECT region_name, id FROM {DIM_REGION_TABLE}"
        region_map_df = pd.read_sql(region_map_query, conn)
        region_map = region_map_df.set_index('region_name')['id'].to_dict()
        
        # Ánh xạ Region Key (ID) vào bảng Fact
        df['gold_region_id'] = df['region_name'].map(region_map)


        # --- C. XỬ LÝ FACT: FACT_GOLD_PRICE ---
        
        fact_insert_query = f"""
        INSERT INTO {FACT_PRICE_TABLE} (gold_type_id, gold_region_id, buy_price, sell_price, spread, capture_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s);
        """

        for index, row in df.iterrows():
            cursor.execute(fact_insert_query, (
                row['gold_type_id'],
                row['gold_region_id'], # <--- Thêm gold_region_id vào đây
                row['buy_price'],
                row['sell_price'],
                row['spread'],
                row['capture_timestamp']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Đã Load thành công {len(df)} bản ghi vào {FACT_PRICE_TABLE} và cập nhật Dimensions.")
        return True

    except Exception as e:
        print(f"LỖI KHI LOAD DỮ LIỆU VÀO DATA WAREHOUSE: {e}")
        return False

if __name__ == "__main__":
    base_directory = os.environ.get("BASE_DIR", ".") 
    
    if run_load_datawarehouse(base_directory):
        sys.exit(0)
    else:
        sys.exit(1)