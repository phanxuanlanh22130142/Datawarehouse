import pandas as pd
import mysql.connector
import os
import sys

# Tên bảng tổng hợp trong Data Warehouse để đọc
AGGREGATE_TABLE_NAME = "AGG_DAILY_GOLD_SUMMARY"

# Tên bảng trong Data Mart để ghi dữ liệu cuối cùng
DATAMART_TABLE_NAME = "DM_GOLD_PRICE_REPORT"


def connect_db(db_name):
    """Kết nối MySQL sử dụng thông số trong Docker Compose."""
    return mysql.connector.connect(
        host="db_staging",         # Tên service MySQL trong docker-compose.yml
        database=db_name,          # db_warehouse hoặc db_datamart
        user="root",
        password=os.environ.get("MYSQL_ROOT_PASSWORD")  # <<<<<< SỬA ĐÚNG Ở ĐÂY
    )


def run_load_datamart():
    """Load dữ liệu DW → Data Mart."""

    try:
        print("Đang kết nối đến Data Warehouse...")

        dw_conn = connect_db("db_warehouse")
        query = f"SELECT * FROM {AGGREGATE_TABLE_NAME}"

        df_final = pd.read_sql(query, dw_conn, parse_dates=['capture_date'])
        dw_conn.close()

        if df_final.empty:
            print("⚠ Không có dữ liệu tổng hợp trong DW.")
            return True

        print(f"DW có {len(df_final)} bản ghi — chuẩn bị load vào DM...")

        dm_conn = connect_db("db_datamart")
        cursor = dm_conn.cursor()

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {DATAMART_TABLE_NAME} (
            brand_name VARCHAR(255),
            capture_date DATE,
            avg_buy_price DECIMAL(10, 0),
            max_sell_price DECIMAL(10, 0),
            min_sell_price DECIMAL(10, 0),
            avg_spread DECIMAL(10, 0),
            PRIMARY KEY (brand_name, capture_date)
        );
        """
        cursor.execute(create_table_sql)

        insert_sql = f"""
        INSERT INTO {DATAMART_TABLE_NAME}
        (brand_name, capture_date, avg_buy_price, max_sell_price, min_sell_price, avg_spread)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            avg_buy_price = VALUES(avg_buy_price),
            max_sell_price = VALUES(max_sell_price),
            min_sell_price = VALUES(min_sell_price),
            avg_spread = VALUES(avg_spread);
        """

        for _, row in df_final.iterrows():
            cursor.execute(insert_sql, (
                row['brand_name'],
                row['capture_date'].strftime('%Y-%m-%d'),
                row['avg_buy_price'],
                row['max_sell_price'],
                row['min_sell_price'],
                row['avg_spread']
            ))

        dm_conn.commit()
        cursor.close()
        dm_conn.close()

        print(f"🎉 Load thành công {len(df_final)} bản ghi vào Data Mart ({DATAMART_TABLE_NAME}).")
        return True

    except Exception as e:
        print(f"❌ LỖI KHI LOAD DATA MART: {e}")
        return False


if __name__ == "__main__":
    if run_load_datamart():
        sys.exit(0)
    else:
        sys.exit(1)
