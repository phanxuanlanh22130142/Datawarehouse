package com.example.dw;

import java.sql.Timestamp;
import java.math.BigDecimal; // Thêm thư viện BigDecimal để xử lý tiền tệ chính xác hơn

public class GoldPriceData {
    // Nên sử dụng BigDecimal thay vì double cho các giá trị tiền tệ để tránh lỗi làm tròn số (Floating Point Arithmetic Issues)
    private String brand;
    private BigDecimal buyPrice;
    private BigDecimal sellPrice;
    private Timestamp updateTime;

    // Constructor mặc định (Bắt buộc)
    public GoldPriceData() {}

    // Constructor đầy đủ
    public GoldPriceData(String brand, BigDecimal buyPrice, BigDecimal sellPrice, Timestamp updateTime) {
        this.brand = brand;
        this.buyPrice = buyPrice;
        this.sellPrice = sellPrice;
        this.updateTime = updateTime;
    }

    // --- Getters and Setters ---

    // Lưu ý: Thay đổi kiểu dữ liệu thành BigDecimal
    public BigDecimal getBuyPrice() { return buyPrice; }
    public void setBuyPrice(BigDecimal buyPrice) { this.buyPrice = buyPrice; }

    public BigDecimal getSellPrice() { return sellPrice; }
    public void setSellPrice(BigDecimal sellPrice) { this.sellPrice = sellPrice; }

    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }

    public Timestamp getUpdateTime() { return updateTime; }
    public void setUpdateTime(Timestamp updateTime) { this.updateTime = updateTime; }

    // --- Phương thức Hỗ trợ ---

    /**
     * Phương thức kiểm tra tính hợp lệ cơ bản của bản ghi.
     * Thường dùng trong lớp Transformer để lọc dữ liệu xấu.
     * @return true nếu bản ghi hợp lệ, ngược lại là false.
     */
    public boolean isValid() {
        // Kiểm tra cơ bản:
        // 1. Tên thương hiệu không được null hoặc rỗng.
        // 2. Giá mua và giá bán phải lớn hơn 0.
        // 3. Thời gian cập nhật không được null.
        return brand != null && !brand.trim().isEmpty() &&
                buyPrice != null && buyPrice.compareTo(BigDecimal.ZERO) > 0 &&
                sellPrice != null && sellPrice.compareTo(BigDecimal.ZERO) > 0 &&
                updateTime != null;
    }

    /**
     * Ghi đè phương thức toString() để debug và logging dễ dàng.
     */
    @Override
    public String toString() {
        return "GoldPriceData{" +
                "brand='" + brand + '\'' +
                ", buyPrice=" + buyPrice +
                ", sellPrice=" + sellPrice +
                ", updateTime=" + updateTime +
                '}';
    }
}