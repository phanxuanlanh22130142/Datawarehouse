package com.example.dw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DBWriter {

    // Thông tin kết nối CƠ SỞ DỮ LIỆU ĐÍCH (Thường là PostgreSQL trong Docker)
    // Giả định dịch vụ DB của bạn trong Docker là 'db' và chạy trên cổng 5432
    private static final String DB_URL = "jdbc:postgresql://db:5432/datawarehouse_db";
    private static final String DB_USER = "dw_user"; // Thay thế bằng tên người dùng DB thực tế của bạn
    private static final String DB_PASSWORD = "dw_password"; // Thay thế bằng mật khẩu DB thực tế của bạn

    // Tên bảng đích trong Data Warehouse Core
    private static final String TARGET_TABLE = "price_domestic_latest";

    /**
     * Phương thức chính để MainApp gọi nhằm lưu dữ liệu đã chuẩn hóa vào Database.
     * @param cleanedData Danh sách các đối tượng GoldPriceData đã được Transformer xử lý.
     */
    public void saveToDB(List<GoldPriceData> cleanedData) {
        if (cleanedData == null || cleanedData.isEmpty()) {
            System.out.println("LOG: DBWriter không nhận được dữ liệu để lưu.");
            return;
        }

        System.out.println("LOG: DBWriter nhận được " + cleanedData.size() + " records cho việc lưu trữ.");

        // 1. Kết nối đến Database
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

            // 2. Chuẩn bị câu lệnh SQL
            // Lưu ý: Chúng ta giả định rằng GoldPriceData có phương thức tạo câu lệnh INSERT/UPSERT.
            // Ví dụ này sử dụng câu lệnh INSERT cơ bản
            String insertSQL = "INSERT INTO " + TARGET_TABLE +
                    "(brand_name, buy_price, sell_price, record_time) " +
                    "VALUES (?, ?, ?, ?)";

            // 3. Thực hiện PreparedStatement (giúp an toàn và hiệu suất hơn)
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

                // Vòng lặp để chèn từng bản ghi
                for (GoldPriceData data : cleanedData) {

                    // Thiết lập các tham số cho câu lệnh SQL
                    // ...
// Thiết lập các tham số cho câu lệnh SQL
                    preparedStatement.setString(1, data.getBrand()); // Sửa từ getBrandName() thành getBrand()
                    preparedStatement.setBigDecimal(2, data.getBuyPrice());
                    preparedStatement.setBigDecimal(3, data.getSellPrice());
// SỬA LỖI Ở ĐÂY: Thay getRecordTime() bằng getUpdateTime()
                    preparedStatement.setTimestamp(4, data.getUpdateTime());

// ...

                    // Thêm vào Batch (lô) để chèn hiệu quả hơn
                    preparedStatement.addBatch();
                }

                // 4. Thực thi Batch Insert
                int[] results = preparedStatement.executeBatch();

                int successCount = 0;
                for (int result : results) {
                    // result > 0 nghĩa là insert/update thành công ít nhất 1 dòng
                    if (result > 0) {
                        successCount++;
                    }
                }

                System.out.println("LOG: DBWriter đã lưu thành công " + successCount + " bản ghi vào bảng " + TARGET_TABLE);

                // Cập nhật control_log tại đây (nếu cần)

            } catch (SQLException e) {
                // Xử lý lỗi SQL (ghi vào error_logs)
                System.err.println("ERROR: Lỗi khi thực thi SQL Batch: " + e.getMessage());
                // Ghi chi tiết lỗi vào bảng error_logs
            }

        } catch (SQLException e) {
            // Xử lý lỗi kết nối DB (ghi vào application_logs)
            System.err.println("ERROR: Không thể kết nối đến Database: " + e.getMessage());
            // Ghi chi tiết lỗi vào bảng error_logs
        } catch (Exception e) {
            // Xử lý lỗi chung
            System.err.println("ERROR: Lỗi chung trong DBWriter: " + e.getMessage());
        }
    }

    /**
     * Phương thức giả định trong lớp GoldPriceData, bạn cần tự định nghĩa
     */
    // public static class GoldPriceData {
    //    public String getBrandName() { return "SJC"; }
    //    public java.math.BigDecimal getBuyPrice() { return new java.math.BigDecimal("70000000"); }
    //    public java.math.BigDecimal getSellPrice() { return new java.math.BigDecimal("71000000"); }
    //    public java.sql.Timestamp getRecordTime() { return new java.sql.Timestamp(System.currentTimeMillis()); }
    // }
}