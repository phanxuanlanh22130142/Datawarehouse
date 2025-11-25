from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
import os
import pandas as pd
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Thông tin kết nối MySQL (Sử dụng tên Docker Service để Flask chạy trong container)
# LƯU Ý: Đã đổi tên host trong Docker Compose của bạn là 'db_staging', nếu bạn dùng 'mysql-staging-db'
# trong docker-compose.yml thì giữ nguyên. Tôi giữ nguyên theo code cũ của bạn.
DB_HOST = "mysql-staging-db" 
DB_USER = os.environ.get("MYSQL_USER", "root")
DB_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root_password_local")
DB_NAME = "db_warehouse" # Hoặc db_datamart nếu bạn Load bảng aggregate vào đó
AGGREGATE_TABLE = "AGG_DAILY_GOLD_SUMMARY" 

def connect_db():
    """Thiết lập kết nối tới MySQL Database."""
    return mysql.connector.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

@app.route('/api/gold-trend', methods=['GET'])
def get_gold_trend():
    """Endpoint API để lấy dữ liệu giá vàng tổng hợp."""
    try:
        conn = connect_db()
        
        # Truy vấn: Lấy 30 bản ghi gần nhất (mỗi bản ghi là giá trung bình theo ngày/thương hiệu)
        query = f"""
        SELECT 
            capture_date, 
            brand_name, 
            avg_buy_price 
        FROM {AGGREGATE_TABLE}
        ORDER BY capture_date DESC
        LIMIT 30;
        """
        
        # SỬA LỖI: Thêm parse_dates để buộc Pandas đọc cột này dưới dạng datetime
        df = pd.read_sql(query, conn, parse_dates=['capture_date']) 
        conn.close()
        
        # Chuyển DataFrame sang định dạng JSON
        # Bây giờ .dt accessor hoạt động chính xác
        df['capture_date'] = df['capture_date'].dt.strftime('%Y-%m-%d')
        
        return jsonify(df.to_dict('records'))

    except Exception as e:
        print(f"LỖI API: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Endpoint trả về trạng thái cập nhật cuối cùng."""
    # Giả sử thời gian cập nhật là thời gian chạy API
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return jsonify({"last_update": now})


if __name__ == '__main__':
    # Chạy Flask ở port 5000 (cần mapping trong docker-compose.yml)
    app.run(host='0.0.0.0', port=5000, debug=True)