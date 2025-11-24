import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import sys
import os
import numpy as np

TARGET_URL = "https://giavang.org/"
CSS_SELECTOR_TABLE = "table.table-bordered" 

def clean_price_text(price_text):
    """Chuẩn hóa sơ cấp: Loại bỏ dấu phân cách và đơn vị (chỉ giữ số)."""
    if price_text:
        return re.sub(r'[^0-9]', '', price_text).strip()
    return ''

def run_gold_extractor(base_dir):
    today_str = datetime.now().strftime("%d%m%Y")
    file_name = f"giavang_{today_str}.csv"
    output_path = os.path.join(base_dir, 'data', 'raw', file_name) 

    try:
        response = requests.get(TARGET_URL, timeout=10)
        response.raise_for_status() 
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.select_one(CSS_SELECTOR_TABLE)
        if not table:
            print("LỖI EXTRACTOR: Không tìm thấy bảng.")
            return False

        data_rows = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Biến tạm để lưu Khu vực hiện tại
        current_region = "" 

        rows = table.find_all('tr')
        if rows:
            rows = rows[1:] # Bỏ qua header row
        
        for row in rows:
            cols = row.find_all('td')
            
            # --- LOGIC XỬ LÝ ROWSPAN MỚI ---
            
            if not cols:
                continue

            # Vị trí bắt đầu của dữ liệu (Brand)
            brand_index = 0
            
            # Nếu ô đầu tiên có thuộc tính rowspan, đó là ô Khu vực mới (4 cột)
            if cols[0].has_attr('rowspan'):
                
                new_region = cols[0].text.strip()
                if new_region:
                    current_region = new_region # Lưu Khu vực mới
                
                # Dữ liệu Brand bắt đầu từ cột thứ 2 (chỉ số 1)
                brand_index = 1
                
            # Nếu không có rowspan, nhưng có 3 cột, dữ liệu Brand bắt đầu từ cột 0
            # Dùng Khu vực đã lưu (current_region)
            elif len(cols) == 3:
                brand_index = 0
            
            # Nếu không rơi vào 2 trường hợp trên (ví dụ: dòng trống hoặc cấu trúc lạ)
            else:
                continue

            # Đảm bảo có đủ 3 cột dữ liệu (Brand, Buy, Sell)
            if len(cols) > brand_index + 2:
                brand = cols[brand_index].text.strip()
                cleaned_buy = clean_price_text(cols[brand_index + 1].text)
                cleaned_sell = clean_price_text(cols[brand_index + 2].text)
            else:
                continue

            # --- THÊM DỮ LIỆU VÀO DANH SÁCH ---
            # Chỉ thêm vào nếu brand_name và region_name không trống
            if brand and current_region:
                data_rows.append({
                    'region_name': current_region, 
                    'brand_name': brand,
                    'buy_price_raw': cleaned_buy, 
                    'sell_price_raw': cleaned_sell,
                    'timestamp': current_time
                })
            

        # Ghi dữ liệu vào đường dẫn output động
        df = pd.DataFrame(data_rows)
        
        # Loại bỏ các dòng mà brand_name hoặc buy_price bị trống
        df.replace('', np.nan, inplace=True)
        df.dropna(subset=['brand_name', 'buy_price_raw'], inplace=True)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"Đã trích xuất thành công: {output_path}. Số dòng: {len(df)}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"LỖI KẾT NỐI: Không thể truy cập {TARGET_URL}. {e}")
        return False
    except Exception as e:
        print(f"LỖI CHUNG KHI TRÍCH XUẤT: {e}")
        return False

if __name__ == "__main__":
    base_directory = os.environ.get("BASE_DIR", ".") 
    
    if run_gold_extractor(base_directory):
        sys.exit(0)
    else:
        sys.exit(1)