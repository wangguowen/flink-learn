package com.flink.learn.demo.data;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class CreateUserUrlData {

    public static void main(String[] args) throws Exception {
        String fileName = "E:\\test_data\\user_log.txt";
        CreateUserUrlData userUrl = new CreateUserUrlData();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 1; i < 101; i++) {
            String user = getRandomUser();
            String url = getRandomURL();
            int visitCount = getRandomVisitCount();
            long currentTimestamp = System.currentTimeMillis();
            String ts = sdf.format(new Date(currentTimestamp));
            long sleepTime = new Double(Math.random() * 5).longValue();
            userUrl.writeInFile(fileName, user + "," + url + "," + visitCount + "," + currentTimestamp + "," + ts);
            Thread.sleep(sleepTime * 1000);
        }
    }

    public static String getRandomUser() {
        ArrayList<String> array = new ArrayList<>();
        array.add("小明");
        array.add("楠楠");
        array.add("丹丹");
        array.add("咕宝");
        array.add("玛丽");
        int index = (int) (Math.random() * 5);
        return array.get(index);
    }

    public static String getRandomURL() {
        ArrayList<String> array = new ArrayList<>();
        array.add("https://www.baidu.com");
        array.add("https://www.iqiyi.com");
        array.add("https://www.mgtv.com");
        array.add("https://www.bilibili.com");
        array.add("https://www.huya.com");
        int index = (int) (Math.random() * 5);
        return array.get(index);
    }

    public static int getRandomVisitCount() {
        int num = (int) (Math.random() * 1000 + 1);
        return num;
    }

    public void writeInFile(String filePath, String content) {
        BufferedWriter out = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }

            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write(content);
            out.write("\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
