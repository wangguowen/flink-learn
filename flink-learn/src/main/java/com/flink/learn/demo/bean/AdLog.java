package com.flink.learn.demo.bean;

public class AdLog {

    private String userId; //2

    private String msgType; //6

    private int positionId; //4

    private Long timestamp; //5

    private String time;

    private String packageName; //7

    private String ip; //12

    private String net; //10

    private String deviceModel; //11

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public void setPositionId(int positionId) {
        this.positionId = positionId;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getUserId() {
        return userId;
    }

    public String getMsgType() {
        return msgType;
    }

    public int getPositionId() {
        return positionId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getIp() {
        return ip;
    }

    public String getNet() {
        return net;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public String getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "AdLog{" +
                "userId='" + userId + '\'' +
                ", msgType='" + msgType + '\'' +
                ", positionId=" + positionId +
                ", timestamp=" + timestamp +
                ", time='" + time + '\'' +
                ", packageName='" + packageName + '\'' +
                ", ip='" + ip + '\'' +
                ", net='" + net + '\'' +
                ", deviceModel='" + deviceModel + '\'' +
                '}';
    }
}
