import pandas as pd
import mysql.connector
import os
import sys
from datetime import datetime

# ----------------------------------------------------
# 1. THÔNG TIN KẾT NỐI VÀ ĐƯỜNG DẪN
# ----------------------------------------------------
# Trong scripts/load_staging.py:
# ...
DB_HOST = "db_staging" # Đã sửa ở bước trước
DB_DATABASE = "db_staging"
DB_USER = "root" # <--- SỬA LẠI THÀNH ROOT
DB_PASSWORD = "root_password_local" # <--- SỬA LẠI THÀNH MẬT KHẨU CỦA BẠN
# ...

def run_load_staging(base_dir):
    """
    Đọc dữ liệu từ file CSV thô và chèn vào bảng gold_raw trong db_staging.
    """
    
    # 1. ĐỊNH DẠNG ĐƯỜNG DẪN FILE INPUT
    today_str = datetime.now().strftime("%d%m%Y")
    file_name = f"giavang_{today_str}.csv"
    input_path = os.path.join(base_dir, 'data', 'raw', file_name)

    if not os.path.exists(input_path):
        print(f"LỖI LOAD STAGING: Không tìm thấy file RAW tại {input_path}")
        return False

    try:
        # 2. ĐỌC DỮ LIỆU TỪ FILE CSV
        df = pd.read_csv(input_path)
        
        # 3. CHUẨN BỊ DỮ LIỆU ĐỂ CHÈN (MAP CỘT VỚI SCHEMA)
        # Sơ đồ bảng gold_raw trong db_staging:
        # (region, gold_type, buy_raw, sell_raw, updated_time, source_url)
        
        # Định nghĩa Source URL (có thể là hằng số)
        SOURCE_URL = "https://giavang.org/sjc" 
        
        # Thêm các cột cần thiết cho bảng gold_raw
        df['gold_type'] = df['brand_name']
        df['updated_time'] = df['timestamp']
        df['source_url'] = SOURCE_URL
        df['gold_item'] = 'Vàng 9999' # Giả định cho giá vàng SJC/PNJ/DOJI...

        # Danh sách các cột TỪ DATAFRAME sẽ chèn vào DB
        COLUMNS_TO_INSERT = [
            'region_name',      # Tương ứng với cột 'region' trong DB
            'gold_type',
            'gold_item',
            'buy_price_raw',
            'sell_price_raw',
            'updated_time',
            'source_url'
        ]
        
        df_insert = df[COLUMNS_TO_INSERT]
        
        # 4. KẾT NỐI VỚI MYSQL
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        cursor = conn.cursor()

        # 5. XÂY DỰNG LỆNH CHÈN
        # Tên cột trong SQL phải khớp với tên trong bảng 
        sql_insert = """
INSERT INTO gold_raw 
(region, gold_type, gold_item, buy_raw, sell_raw, updated_time, source_url) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
        
        # Chuyển DataFrame thành danh sách các tuple để chèn
        records_to_insert = [tuple(row) for row in df_insert.values]

        # 6. THỰC HIỆN CHÈN VÀ COMMIT
        cursor.executemany(sql_insert, records_to_insert)
        conn.commit()

        print(f"Đã chèn thành công {cursor.rowcount} bản ghi vào db_staging.gold_raw")
        
        cursor.close()
        conn.close()
        return True

    except mysql.connector.Error as err:
        print(f"LỖI KẾT NỐI/MYSQL: {err}")
        return False
    except Exception as e:
        print(f"LỖI CHUNG KHI LOAD STAGING: {e}")
        return False

if __name__ == "__main__":
    base_directory = os.environ.get("BASE_DIR", ".") 
    if run_load_staging(base_directory):
        sys.exit(0)
    else:
        sys.exit(1)