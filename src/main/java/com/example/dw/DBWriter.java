package com.example.dw;

import java.util.List;
// Import các thư viện SQL để kết nối DB, ví dụ:
// import java.sql.Connection;
// import java.sql.DriverManager;

public class DBWriter {

    // Phương thức BẮT BUỘC để MainApp gọi
    public void saveToDB(List<GoldPriceData> cleanedData) {
        // Đây là nơi bạn sẽ viết code để kết nối với PostgreSQL (dịch vụ 'db' trong Docker Compose)
        // và lưu dữ liệu.

        System.out.println("LOG: DBWriter received " + cleanedData.size() + " records for saving.");
        // Code thực tế để kết nối và insert data vào DB sẽ nằm ở đây.
        // Tạm thời, chúng ta để nó trống để biên dịch thành công.
    }
}