package com.example.dw;

// THÊM: Import lớp List chuẩn của Java Collections
import java.util.List;
import java.util.ArrayList; // Thường cần để khởi tạo

// Các lớp khác trong cùng package không cần import, như bạn đã note đúng:
// GoldPriceScraper, GoldPriceData, Transformer, DBWriter

public class MainApp {
    public static void main(String[] args) {
        // 1. SỬA TÊN LỚP: PriceScraper -> GoldPriceScraper
        GoldPriceScraper scraper = new GoldPriceScraper();

        // 2. SỬA KIỂU DỮ LIỆU: List<GoldPrice> -> List<GoldPriceData>
        List<GoldPriceData> data = scraper.getGoldPrices(); // Giả sử method trả về GoldPriceData

        Transformer transformer = new Transformer();
        List<GoldPriceData> cleanedData = transformer.clean(data); // Đảm bảo Transformer cũng dùng GoldPriceData

        DBWriter writer = new DBWriter();
        writer.saveToDB(cleanedData);
    }
}
