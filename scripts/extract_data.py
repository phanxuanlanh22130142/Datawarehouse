import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import sys
import os

# Địa chỉ nguồn dữ liệu
TARGET_URL = "https://giavang.org/"

# Selector bạn đã tìm thấy và hoạt động tốt
CSS_SELECTOR_TABLE = "table.table-bordered" # HOẶC "table.table-striped"

def clean_price_text(price_text):
    """Chuẩn hóa sơ cấp: Loại bỏ dấu phân cách và đơn vị (chỉ giữ số)."""
    if price_text:
        return re.sub(r'[^0-9]', '', price_text).strip()
    return ''

def run_gold_extractor(base_dir): 
    """Trích xuất dữ liệu và ghi vào thư mục RAW với tên file động (giavang_ddmmyy.csv)."""
    
    # 1. ĐỊNH DẠNG TÊN FILE VÀ ĐƯỜNG DẪN OUTPUT (Theo kiến trúc 6 bước)
    today_str = datetime.now().strftime("%d%m%Y")
    file_name = f"giavang_{today_str}.csv"
    
    # Đường dẫn output: [base_dir]/data/raw/giavang_ddmmyy.csv
    output_path = os.path.join(base_dir, 'data', 'raw', file_name) 

    try:
        response = requests.get(TARGET_URL, timeout=10)
        response.raise_for_status() 
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.select_one(CSS_SELECTOR_TABLE)
        
        if not table:
            print(f"LỖI EXTRACTOR: Không tìm thấy bảng với selector: {CSS_SELECTOR_TABLE}")
            return False

        data_rows = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = table.find_all('tr')
        if rows:
            rows = rows[1:] 
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 3: 
                brand = cols[0].text.strip()
                cleaned_buy = clean_price_text(cols[1].text)
                cleaned_sell = clean_price_text(cols[2].text)
                
                data_rows.append({
                    'brand_name': brand,
                    'buy_price_raw': cleaned_buy, 
                    'sell_price_raw': cleaned_sell,
                    'timestamp': current_time
                })

        # Ghi dữ liệu vào đường dẫn output động
        df = pd.DataFrame(data_rows)
        # Tạo thư mục data/raw nếu chưa tồn tại
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"Đã trích xuất thành công: {output_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"LỖI KẾT NỐI: Không thể truy cập {TARGET_URL}. {e}")
        return False
    except Exception as e:
        print(f"LỖI CHUNG KHI TRÍCH XUẤT: {e}")
        return False

if __name__ == "__main__":
    # KHÔNG CẦN NHẬN sys.argv[1] NỮA, mà dùng biến môi trường BASE_DIR
    base_directory = os.environ.get("BASE_DIR", ".") 
    
    if run_gold_extractor(base_directory):
        sys.exit(0)
    else:
        sys.exit(1)