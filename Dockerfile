# Sử dụng base image Python chính thức
FROM python:3.10-slim

# Thiết lập thư mục làm việc bên trong container
WORKDIR /app

# Sao chép file requirements.txt vào container
COPY requirements.txt .

# Cài đặt các thư viện cần thiết (pandas, lxml, v.v.)
RUN pip install --no-cache-dir -r requirements.txt

# >>> DÒNG CẦN THÊM VÀO HOẶC KIỂM TRA <<<
# Cài đặt MySQL Client (để có thể chạy lệnh mysql thủ công)
RUN apt-get update && \
    apt-get install -y default-mysql-client && \
    rm -rf /var/lib/apt/lists/*
# >>> KẾT THÚC DÒNG CẦN THÊM VÀO <<<

# Sao chép toàn bộ thư mục dự án vào thư mục /app trong container
COPY . .