import pandas as pd
import numpy as np
import os
import sys
import mysql.connector
from datetime import datetime

# ----------------------------------------------------
# 1. THÔNG TIN KẾT NỐI VÀ ĐƯỜNG DẪN
# ----------------------------------------------------
# Sử dụng tên service Docker 'mysql-staging-db'
DB_HOST = "mysql-staging-db" 
DB_DATABASE = "db_staging"
# Sử dụng biến môi trường (Docker Compose)
DB_USER = os.environ.get("MYSQL_USER", "root") 
DB_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root_password_local")

# Thư mục để ghi file đã Transform (đầu vào của Load DW)
CLEANED_DIR = "data/cleaned"
CLEANED_FILE_TEMPLATE = "giavang_cleaned_{}.csv"

def run_transform(base_dir):
    """Đọc dữ liệu từ db_staging, làm sạch, và ghi vào thư mục Cleaned."""
    
    today_str = datetime.now().strftime("%d%m%Y")
    cleaned_file_name = CLEANED_FILE_TEMPLATE.format(today_str)
    output_path = os.path.join(base_dir, CLEANED_DIR, cleaned_file_name)

    try:
        # 1. KẾT NỐI VÀ ĐỌC DỮ LIỆU TỪ DB STAGING
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        
        # CÂU LỆNH SELECT: Lấy dữ liệu thô từ bảng gold_raw
        select_query = """
        SELECT region, gold_type, buy_raw, sell_raw, updated_time 
        FROM gold_raw
        """
        
        # Đọc dữ liệu trực tiếp vào DataFrame
        df = pd.read_sql(select_query, conn)
        conn.close()

        if df.empty:
            print("LỖI TRANSFORM: Bảng gold_raw rỗng hoặc không có dữ liệu để xử lý.")
            return False
            
        # Đổi tên cột cho dễ xử lý trong Transform
        df.rename(columns={'region': 'region_name', 
                           'gold_type': 'brand_name', 
                           'updated_time': 'timestamp'}, inplace=True)

        # ----------------------------------------------------
        # 2. XỬ LÝ DỮ LIỆU (Transform Logic)
        # ----------------------------------------------------
        
        # 2.1. Làm sạch giá trị (Chuyển từ chuỗi sang số nguyên)
        # Xóa dấu phẩy (,) và chuyển thành số nguyên
        df['buy_raw'] = df['buy_raw'].astype(str).str.replace(',', '', regex=False)
        df['sell_raw'] = df['sell_raw'].astype(str).str.replace(',', '', regex=False)

        df['buy_price'] = pd.to_numeric(df['buy_raw'], errors='coerce', downcast='integer')
        df['sell_price'] = pd.to_numeric(df['sell_raw'], errors='coerce', downcast='integer')

        # 2.2. Xử lý thiếu/lỗi dữ liệu (Loại bỏ các dòng có giá trị giá không hợp lệ)
        df.dropna(subset=['buy_price', 'sell_price'], inplace=True)

        # 2.3. Tính toán Spread (Biên độ lợi nhuận)
        df['spread'] = df['sell_price'] - df['buy_price']
        
        # 2.4. Làm sạch Khu vực và Thương hiệu
        df['region_name'] = df['region_name'].astype(str).str.strip()
        df['brand_name'] = df['brand_name'].astype(str).str.strip()

        # 2.5. Tạo cột Date (Ngày trích xuất)
        df['capture_timestamp'] = pd.to_datetime(df['timestamp'])
        
        # ----------------------------------------------------
        # 3. CHỌN CÁC CỘT CUỐI CÙNG (MODEL SẴN SÀNG CHO DW)
        # ----------------------------------------------------
        df_final = df[[
            'region_name', 
            'brand_name',
            'buy_price',
            'sell_price',
            'spread',
            'capture_timestamp'
        ]]

        # 4. GHI DỮ LIỆU ĐÃ TRANSFORM VÀO THƯ MỤC CLEANED
        # Đảm bảo thư mục output tồn tại
        os.makedirs(os.path.join(base_dir, CLEANED_DIR), exist_ok=True)
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"Đã Transform thành công {len(df_final)} bản ghi. File ghi tại: {output_path}")
        return True

    except Exception as e:
        print(f"LỖI KHI THỰC HIỆN TRANSFORM: {e}")
        # Ghi lỗi chi tiết hơn nếu cần
        # import traceback
        # print(traceback.format_exc()) 
        return False

if __name__ == "__main__":
    # Lấy thư mục gốc từ biến môi trường (mặc định là thư mục hiện tại)
    base_directory = os.environ.get("BASE_DIR", ".")
    if run_transform(base_directory):
        sys.exit(0)
    else:
        sys.exit(1)