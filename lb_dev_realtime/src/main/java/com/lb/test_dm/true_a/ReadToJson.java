package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ReadToJson {
    public static void main(String[] args) throws IOException {
        String filePath = "D:\\PM\\idea-pm\\sensitive-words-realtime-dev-liubo2\\docs\\txt/timeWeight.txt";
        HashMap<String, JSONObject> map = readFileToJsonMap(filePath);
        System.out.println(map);
        map.forEach((k,v)->{
            System.out.println(v.getString("40-49"));

        });


//        JSONObject object = JSON.parseObject(jsonString);
//        System.out.println(object);
    }
    public static HashMap<String,JSONObject> readFileToJsonMap(String filePath)  {

        HashMap<String,JSONObject> map=new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                 JSONObject object = JSON.parseObject(line);

                for (String s : object.keySet()) {
                    String values = object.getString(s );
                    JSONObject objects = JSON.parseObject(values);
                    map.put(s,objects);
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return map;
    }
}
