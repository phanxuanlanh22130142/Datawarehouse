package com.example.dw;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class GoldPriceScraper {

    // URL nguồn dữ liệu (có thể là API hoặc trang HTML cần scrape)
    private static final String DATA_SOURCE_URL = "https://giavang.org";

    /**
     * Phương thức trích xuất dữ liệu giá vàng từ nguồn.
     * Đây là bước 'Extract' trong quy trình ETL.
     * @return Danh sách các đối tượng GoldPriceData thô.
     */
    public List<GoldPriceData> getGoldPrices() {
        List<GoldPriceData> rawDataList = new ArrayList<>();

        System.out.println("LOG: Bắt đầu trích xuất dữ liệu từ: " + DATA_SOURCE_URL);

        // 1. Tạo HttpClient (Java 11+)
        HttpClient client = HttpClient.newBuilder().build();

        // 2. Tạo HttpRequest
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(DATA_SOURCE_URL))
                .header("User-Agent", "Mozilla/5.0") // Thêm User-Agent để tránh bị từ chối
                .timeout(java.time.Duration.ofSeconds(20))
                .build();

        try {
            // 3. Gửi Request và nhận Response
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String rawContent = response.body();

                // 4. PHÂN TÍCH CÚ PHÁP (PARSING) DỮ LIỆU THÔ
                System.out.println("LOG: Đã nhận được nội dung. Bắt đầu phân tích cú pháp...");

                // >>>>>>> THAY THẾ PHẦN NÀY BẰNG LOGIC PHÂN TÍCH CÚ PHÁP THỰC TẾ <<<<<<<

                // Ví dụ Giả định (bạn sẽ thay thế bằng logic Jsoup/Jackson/GSON của bạn)
                rawDataList = parseRawContent(rawContent);

                // >>>>>>> KẾT THÚC PHẦN GIẢ ĐỊNH <<<<<<<

                System.out.println("LOG: Phân tích cú pháp hoàn tất. Trích xuất được " + rawDataList.size() + " bản ghi.");

            } else {
                System.err.println("ERROR: Lỗi HTTP - Không thể trích xuất dữ liệu. Mã trạng thái: " + response.statusCode());
                // Ghi log lỗi vào application_logs
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("ERROR: Lỗi kết nối/ngắt quãng khi truy cập nguồn: " + e.getMessage());
            // Ghi log lỗi vào application_logs
            Thread.currentThread().interrupt();
        }

        return rawDataList;
    }

    /**
     * Phương thức Giả định để phân tích cú pháp nội dung thô (ví dụ: HTML/JSON).
     * Cần được thay thế bằng logic thực tế.
     */
    private List<GoldPriceData> parseRawContent(String rawContent) {
        List<GoldPriceData> list = new ArrayList<>();

        // GIẢ ĐỊNH: Nếu nội dung là HTML, bạn sẽ dùng Jsoup tại đây.
        // Nếu nội dung là JSON/XML, bạn sẽ dùng Jackson/GSON tại đây.

        // Hiện tại, chúng ta chỉ tạo ra một số dữ liệu mẫu (MOCK DATA) để kiểm tra luồng.

        // Ví dụ 1: Giá Vàng SJC
        GoldPriceData sjcData = new GoldPriceData(
                "SJC",
                new BigDecimal("70500000"), // Giá mua
                new BigDecimal("71500000"), // Giá bán
                new Timestamp(System.currentTimeMillis())
        );
        list.add(sjcData);

        // Ví dụ 2: Giá Vàng PNJ
        GoldPriceData pnjData = new GoldPriceData(
                "PNJ",
                new BigDecimal("59000000"),
                new BigDecimal("60000000"),
                new Timestamp(System.currentTimeMillis())
        );
        list.add(pnjData);

        return list;
    }
}