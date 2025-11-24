package com.example.dw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class PricePredictor {

    // Thông tin kết nối CƠ SỞ DỮ LIỆU (Tái sử dụng từ DBWriter)
    private static final String DB_URL = "jdbc:postgresql://db:5432/datawarehouse_db";
    private static final String DB_USER = "dw_user";
    private static final String DB_PASSWORD = "dw_password";

    // Tên bảng đích cho Dữ liệu Tóm tắt (Summary Data/Data Marts)
    private static final String SUMMARY_TABLE = "gold_price_summary";

    /**
     * Phương thức thực hiện tính toán nâng cao (ví dụ: Giá trung bình)
     * và lưu kết quả vào bảng Summary Data.
     * @return true nếu quá trình tính toán và lưu trữ thành công.
     */
    public boolean generateSummaryData() {
        System.out.println("LOG: Bắt đầu tính toán dữ liệu tóm tắt (Summary Data).");

        // 1. SQL để TÍNH TOÁN GIÁ TRUNG BÌNH từ bảng Fact chính
        String analysisSQL = "SELECT brand_name, " +
                "AVG(buy_price) AS avg_buy, " +
                "AVG(sell_price) AS avg_sell " +
                "FROM price_domestic_latest " + // Lấy dữ liệu từ bảng Fact chính
                "GROUP BY brand_name";

        // 2. SQL để UPSERT (Update/Insert) kết quả vào bảng Summary
        String upsertSQL = "INSERT INTO " + SUMMARY_TABLE +
                " (brand_name, average_buy_price, average_sell_price, calculated_at) " +
                " VALUES (?, ?, ?, NOW()) " +
                " ON CONFLICT (brand_name) DO UPDATE " +
                " SET average_buy_price = EXCLUDED.average_buy_price, " +
                "     average_sell_price = EXCLUDED.average_sell_price, " +
                "     calculated_at = EXCLUDED.calculated_at";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

            // Bước 2a: Truy vấn dữ liệu từ DW Core
            try (PreparedStatement selectStmt = connection.prepareStatement(analysisSQL);
                 ResultSet rs = selectStmt.executeQuery();
                 PreparedStatement upsertStmt = connection.prepareStatement(upsertSQL)) {

                int count = 0;

                // Bước 2b: Xử lý từng kết quả và thêm vào Batch để lưu vào bảng Summary
                while (rs.next()) {
                    String brand = rs.getString("brand_name");
                    // Sử dụng BigDecimal với quy tắc làm tròn (setScale)
                    BigDecimal avgBuy = rs.getBigDecimal("avg_buy").setScale(0, RoundingMode.HALF_UP);
                    BigDecimal avgSell = rs.getBigDecimal("avg_sell").setScale(0, RoundingMode.HALF_UP);

                    // Thiết lập tham số cho câu lệnh UPSERT
                    upsertStmt.setString(1, brand);
                    upsertStmt.setBigDecimal(2, avgBuy);
                    upsertStmt.setBigDecimal(3, avgSell);

                    upsertStmt.addBatch();
                    count++;
                }

                // Bước 2c: Thực thi Batch Upsert
                if (count > 0) {
                    upsertStmt.executeBatch();
                    System.out.println("LOG: Đã tính toán và cập nhật thành công " + count + " bản ghi tóm tắt vào bảng " + SUMMARY_TABLE);
                    return true;
                } else {
                    System.out.println("LOG: Không có dữ liệu để tính toán tóm tắt.");
                    return false;
                }

            } catch (SQLException e) {
                System.err.println("ERROR: Lỗi SQL trong PricePredictor: " + e.getMessage());
                // Ghi log lỗi
                return false;
            }

        } catch (SQLException e) {
            System.err.println("ERROR: Không thể kết nối đến Database trong PricePredictor: " + e.getMessage());
            // Ghi log lỗi
            return false;
        }
    }
}