package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ReadToJson {
    public static void main(String[] args) throws IOException {
        //指定文件路径。
        String filePath = "D:\\PM\\idea-pm\\sensitive-words-realtime-dev-liubo2\\docs\\txt/timeWeight.txt";
        //读取文件内容并将其转换为 HashMap，键为字符串，值为 JSONObject。
        HashMap<String, JSONObject> map = readFileToJsonMap(filePath);
        //打印生成的 HashMap。
        System.out.println(map);
        //遍历 HashMap，打印每个 JSONObject 中 "40-49" 对应的字符串值。
        map.forEach((k,v)->{
            System.out.println(v.getString("40-49"));

        });


//        JSONObject object = JSON.parseObject(jsonString);
//        System.out.println(object);
    }


    public static HashMap<String,JSONObject> readFileToJsonMap(String filePath)  {

        //初始化一个空的HashMap用于存储读取的JSON对象。
        HashMap<String,JSONObject> map=new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            //逐行读取文件内容，将每一行解析为JSON对象。
            while ((line = reader.readLine()) != null) {
                JSONObject object = JSON.parseObject(line);

                //遍历JSON对象的键，将每个键对应的值再次解析为JSON对象并存入HashMap。
                for (String s : object.keySet()) {
                    String values = object.getString(s );
                    JSONObject objects = JSON.parseObject(values);
                    map.put(s,objects);
                }
            }
            //捕获并处理可能发生的IO异常。
        }catch (IOException e){
            e.printStackTrace();
        }
        //返回存储了所有JSON对象的HashMap。
        return map;
    }

}
