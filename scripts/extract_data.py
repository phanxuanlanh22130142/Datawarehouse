import pandas as pd
from datetime import datetime
import re
import sys
import os

# Địa chỉ nguồn dữ liệu
TARGET_URL = "https://giavang.org/"

def clean_price_text(price_text):
    """Chuẩn hóa: Loại bỏ dấu phân cách, đơn vị và chuyển sang chuỗi số."""
    if price_text:
        # Giữ lại số và dấu chấm, sau đó loại bỏ dấu chấm (nếu có) và chữ cái
        cleaned = re.sub(r'[^0-9.]', '', price_text).strip()
        # Loại bỏ dấu chấm nếu có
        return cleaned.replace('.', '')
    return ''

def run_gold_extractor(base_dir): 
    """Trích xuất dữ liệu, sử dụng pandas.read_html để xử lý cấu trúc bảng phức tạp."""
    
    today_str = datetime.now().strftime("%d%m%Y")
    file_name = f"giavang_{today_str}.csv"
    output_path = os.path.join(base_dir, 'data', 'raw', file_name) 

    try:
        # Sử dụng pandas.read_html để đọc tất cả các bảng trên trang
        # engine='lxml' sử dụng lxml để phân tích HTML, nhanh và ổn định hơn
        tables = pd.read_html(TARGET_URL, flavor='lxml')
        
        # Bảng giá vàng chính là bảng đầu tiên (chỉ mục 0)
        # Chúng ta cần kiểm tra lại tiêu đề của bảng này, nếu không phải bảng đầu tiên thì chỉnh lại chỉ mục.
        # Thường thì nó là bảng lớn nhất và quan trọng nhất.
        if not tables:
            print("LỖI EXTRACTOR: Không tìm thấy bảng nào trên trang.")
            return False
        
        df = tables[0].copy() # Copy bảng đầu tiên (Bảng giá SJC, PNJ, DOJI)
        
        # 1. ĐẶT LẠI TÊN CỘT ĐÃ BIẾT
        # Tên cột trong bảng HTML là: Khu vực, Hệ thống, Mua vào, Bán ra
        df.columns = ['region_name_raw', 'brand_name_raw', 'buy_price_raw', 'sell_price_raw']
        
        # 2. XỬ LÝ ROWSPAN (Điền giá trị Khu vực bị thiếu)
        # Pandas đọc ô gộp (rowspan) bằng cách điền NaN vào các dòng tiếp theo.
        # Hàm fillna(method='ffill') sẽ điền giá trị Khu vực trước đó vào các ô NaN
        df['region_name'] = df['region_name_raw'].fillna(method='ffill')
        
        # 3. LÀM SẠCH VÀ CHỌN CỘT
        
        # Áp dụng hàm làm sạch giá
        df['buy_price_raw'] = df['buy_price_raw'].apply(clean_price_text)
        df['sell_price_raw'] = df['sell_price_raw'].apply(clean_price_text)
        
        # Đặt timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['timestamp'] = current_time
        
        # Chọn các cột cuối cùng
        df_final = df[[
            'region_name', 
            'brand_name_raw', 
            'buy_price_raw', 
            'sell_price_raw', 
            'timestamp'
        ]]
        
        # Đổi tên cột cho phù hợp với schema
        df_final.columns = [
            'region_name', 
            'brand_name', 
            'buy_price_raw', 
            'sell_price_raw', 
            'timestamp'
        ]

        # Lọc bỏ các dòng không có giá trị
        df_final.dropna(subset=['brand_name', 'buy_price_raw'], inplace=True)
        
        # Ghi dữ liệu vào đường dẫn output động
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"Đã trích xuất thành công bằng Pandas: {output_path}. Số dòng: {len(df_final)}")
        return True

    except ValueError as e:
        print(f"LỖI READ_HTML: Không thể tìm thấy bảng HTML. {e}")
        return False
    except Exception as e:
        print(f"LỖỖ CHUNG KHI TRÍCH XUẤT: {e}")
        return False

if __name__ == "__main__":
    if run_gold_extractor(os.environ.get("BASE_DIR", ".")):
        sys.exit(0)
    else:
        sys.exit(1)