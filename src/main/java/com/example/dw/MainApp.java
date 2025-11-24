package com.example.dw;

import java.util.List;
import java.util.ArrayList; // Thêm để tránh lỗi nếu các lớp khác trả về null

public class MainApp {

    public static void main(String[] args) {
        System.out.println("==================================================");
        System.out.println("== BẮT ĐẦU QUY TRÌNH ETL DỮ LIỆU VÀNG (DW) V1.0 ==");
        System.out.println("==================================================");

        List<GoldPriceData> rawData = new ArrayList<>();
        List<GoldPriceData> cleanedData = new ArrayList<>();

        // --- BƯỚC 1: EXTRACT (Trích xuất) ---
        try {
            GoldPriceScraper scraper = new GoldPriceScraper();
            rawData = scraper.getGoldPrices();
            System.out.println("LOG: Trích xuất hoàn tất. Nhận được " + rawData.size() + " bản ghi thô.");
        } catch (Exception e) {
            System.err.println("FATAL ERROR: Lỗi trong quá trình Extract (GoldPriceScraper). " + e.getMessage());
            // Ghi log lỗi nghiêm trọng và THOÁT
            return;
        }

        // --- BƯỚC 2: TRANSFORM (Biến đổi) ---
        if (!rawData.isEmpty()) {
            try {
                Transformer transformer = new Transformer();
                cleanedData = transformer.clean(rawData);
                System.out.println("LOG: Transform hoàn tất. Còn lại " + cleanedData.size() + " bản ghi hợp lệ.");
            } catch (Exception e) {
                System.err.println("FATAL ERROR: Lỗi trong quá trình Transform. " + e.getMessage());
                // Ghi log lỗi nghiêm trọng và THOÁT
                return;
            }
        } else {
            System.out.println("LOG: Không có dữ liệu để Transform. Bỏ qua bước 2 và 3.");
        }

        // --- BƯỚC 3: LOAD (Tải) ---
        if (!cleanedData.isEmpty()) {
            try {
                DBWriter writer = new DBWriter();
                writer.saveToDB(cleanedData); // Tải vào DW Core
                System.out.println("LOG: Load vào DW Core hoàn tất.");

                // Cập nhật trạng thái vào control_log (Status = FL)
                System.out.println("LOG: Cập nhật control_log: Status = FL (Flow Completed).");

            } catch (Exception e) {
                System.err.println("FATAL ERROR: Lỗi trong quá trình Load (DBWriter). " + e.getMessage());
                // Ghi log lỗi nghiêm trọng và Rollback (nếu cần thiết)
                return;
            }

            // --- BƯỚC 4: PREDICTION/DATA MARTS ---
            // Theo sơ đồ, sau khi tải xong, ta tiến hành tính toán Summary Data/Prediction
            try {
                PricePredictor predictor = new PricePredictor();
                predictor.generateSummaryData();
                System.out.println("LOG: Tính toán Summary Data/Prediction hoàn tất.");
            } catch (Exception e) {
                // Lỗi ở đây không nghiêm trọng bằng ETL, chỉ cần ghi log cảnh báo
                System.err.println("WARNING: Lỗi khi tính toán Summary Data (PricePredictor). " + e.getMessage());
            }
        } else {
            System.out.println("LOG: Không có dữ liệu để Load. Kết thúc luồng.");
        }

        System.out.println("==================================================");
        System.out.println("== QUY TRÌNH ETL KẾT THÚC THÀNH CÔNG ==");
        System.out.println("==================================================");
    }
}