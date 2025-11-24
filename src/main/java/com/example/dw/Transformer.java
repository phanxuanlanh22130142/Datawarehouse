package com.example.dw;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class Transformer {

    /**
     * Phương thức chính thực hiện toàn bộ quá trình Transform dữ liệu thô.
     * @param rawData Danh sách GoldPriceData thô từ GoldPriceScraper.
     * @return Danh sách GoldPriceData đã được làm sạch và chuẩn hóa.
     */
    public List<GoldPriceData> clean(List<GoldPriceData> rawData) {
        if (rawData == null || rawData.isEmpty()) {
            System.out.println("LOG: Transformer không nhận được dữ liệu thô.");
            return new ArrayList<>();
        }

        System.out.println("LOG: Bắt đầu quá trình Transform (" + rawData.size() + " bản ghi).");

        // 1. Lọc và Kiểm tra Chất lượng (Check Quality)
        List<GoldPriceData> validData = filterInvalidRecords(rawData);
        System.out.println("LOG: Đã lọc, còn lại " + validData.size() + " bản ghi hợp lệ.");

        // 2. Chuẩn hóa và Biến đổi (Normalization and Transformation)
        List<GoldPriceData> transformedData = normalizeAndMap(validData);

        System.out.println("LOG: Quá trình Transform hoàn tất.");
        return transformedData;
    }

    /**
     * Bước 1: Lọc dữ liệu KHÔNG hợp lệ.
     * Dữ liệu không hợp lệ được coi là ERROR_LOGS trong sơ đồ của bạn.
     */
    private List<GoldPriceData> filterInvalidRecords(List<GoldPriceData> rawData) {
        // Sử dụng Stream API để lọc nhanh chóng
        return rawData.stream()
                .filter(data -> {
                    boolean isValid = data.isValid(); // Dùng phương thức isValid() đã định nghĩa trong GoldPriceData
                    if (!isValid) {
                        System.err.println("WARNING: Bản ghi không hợp lệ đã bị loại bỏ: " + data.toString());
                        // TẠI ĐÂY: Bạn nên ghi chi tiết bản ghi lỗi này vào bảng 'error_logs'.
                    }
                    return isValid;
                })
                .collect(Collectors.toList());
    }

    /**
     * Bước 2: Chuẩn hóa, làm sạch và ánh xạ dữ liệu (Clean and Map).
     * Bao gồm: Làm sạch chuỗi (trim), làm tròn số, và ánh xạ sang mô hình DW.
     */
    private List<GoldPriceData> normalizeAndMap(List<GoldPriceData> validData) {
        List<GoldPriceData> transformedList = new ArrayList<>();

        for (GoldPriceData data : validData) {

            // 2a. Làm sạch (Cleaning)
            // Loại bỏ khoảng trắng thừa trong tên thương hiệu
            if (data.getBrand() != null) {
                data.setBrand(data.getBrand().trim().toUpperCase()); // Chuẩn hóa thành chữ hoa
            }

            // 2b. Chuẩn hóa Giá (Normalization)
            // Giá vàng thường là số nguyên. Đảm bảo chúng được làm tròn chính xác.
            data.setBuyPrice(data.getBuyPrice().setScale(0, RoundingMode.HALF_UP));
            data.setSellPrice(data.getSellPrice().setScale(0, RoundingMode.HALF_UP));

            // 2c. Ánh xạ (Mapping) - Nếu có thay đổi mô hình
            // (Hiện tại GoldPriceData được dùng cho cả thô và chuẩn hóa,
            // nếu có mô hình riêng cho DW, sẽ ánh xạ tại đây)

            transformedList.add(data);
        }

        return transformedList;
    }
}