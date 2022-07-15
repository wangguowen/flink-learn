package com.flink.learn.demo.bean;

public class Tickets {

    private String buy_user_name;

    private int price;

    private int count;

    private String eventType;

    private String buy_date;

    private Long buy_ts;

    public Tickets() {
    }

    public Tickets(String buy_user_name, int price, int count, String eventType, String buy_date, Long buy_ts) {
        this.buy_user_name = buy_user_name;
        this.price = price;
        this.count = count;
        this.eventType = eventType;
        this.buy_date = buy_date;
        this.buy_ts = buy_ts;
    }

    public String getBuy_user_name() {
        return buy_user_name;
    }

    public void setBuy_user_name(String buy_user_name) {
        this.buy_user_name = buy_user_name;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getBuy_date() {
        return buy_date;
    }

    public void setBuy_date(String buy_date) {
        this.buy_date = buy_date;
    }

    public Long getBuy_ts() {
        return buy_ts;
    }

    public void setBuy_ts(Long buy_ts) {
        this.buy_ts = buy_ts;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "Tickets{" +
                "buy_user_name='" + buy_user_name + '\'' +
                ", price=" + price +
                ", count=" + count +
                ", buy_date='" + buy_date + '\'' +
                ", buy_ts=" + buy_ts +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
