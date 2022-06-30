package com.flink.learn.demo.utils;

import com.flink.learn.demo.bean.AdLog;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtil {

    public static AdLog splitSourceData(String line) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        AdLog adLog = new AdLog();
        String[] arr = line.split(",");
        String userId = arr[1];
        int positionId = Integer.valueOf(arr[3]);
        Long timestamp = Long.valueOf(arr[4]);
        String time = sdf.format(new Date(timestamp));
        String msgType = arr[5]; //6
        String packageName = arr[6]; //7
        String net = arr[9]; //10
        String deviceModel = arr[10]; //11
        String ip = arr[11]; //12
        adLog.setUserId(userId);
        adLog.setPositionId(positionId);
        adLog.setTimestamp(timestamp);
        adLog.setPackageName(packageName);
        adLog.setMsgType(msgType);
        adLog.setDeviceModel(deviceModel);
        adLog.setIp(ip);
        adLog.setNet(net);
        adLog.setTime(time);
        return adLog;
    }
}
