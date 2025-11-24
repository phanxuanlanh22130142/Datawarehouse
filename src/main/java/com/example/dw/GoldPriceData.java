package com.example.dw;

import java.sql.Timestamp;

public class GoldPriceData {
    private String brand;
    private double buyPrice;
    private double sellPrice;
    private Timestamp updateTime;

    // Constructor (Bắt buộc)
    public GoldPriceData() {}

    public GoldPriceData(String brand, double buyPrice, double sellPrice, Timestamp updateTime) {
        this.brand = brand;
        this.buyPrice = buyPrice;
        this.sellPrice = sellPrice;
        this.updateTime = updateTime;
    }

    // Getters and Setters (Bắt buộc để các lớp khác truy cập)
    public double getBuyPrice() { return buyPrice; }
    public void setBuyPrice(double buyPrice) { this.buyPrice = buyPrice; }

    public double getSellPrice() { return sellPrice; }
    public void setSellPrice(double sellPrice) { this.sellPrice = sellPrice; }

    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }

    public Timestamp getUpdateTime() { return updateTime; }
    public void setUpdateTime(Timestamp updateTime) { this.updateTime = updateTime; }

    // Bạn có thể cần thêm các thuộc tính khác tùy theo API bạn cào
}