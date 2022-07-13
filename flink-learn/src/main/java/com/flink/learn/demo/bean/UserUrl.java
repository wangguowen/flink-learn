package com.flink.learn.demo.bean;

public class UserUrl {

    private String userName;

    private String url;

    private int visitCount;

    private Long ts;

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setVisitCount(int visitCount) {
        this.visitCount = visitCount;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getUserName() {
        return userName;
    }

    public String getUrl() {
        return url;
    }

    public int getVisitCount() {
        return visitCount;
    }

    public Long getTs() {
        return ts;
    }

    @Override
    public String toString() {
        return "UserUrl{" +
                "userName='" + userName + '\'' +
                ", url='" + url + '\'' +
                ", visitCount=" + visitCount +
                ", ts=" + ts +
                '}';
    }
}
