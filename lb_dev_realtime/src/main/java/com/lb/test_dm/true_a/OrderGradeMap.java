package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;

public class OrderGradeMap extends RichMapFunction<JSONObject,JSONObject> {

    // 定义各个因素的重要性比例
    private static final double c1Rate = 0.3;
    private static final double tmRate = 0.2;
    private static final double timeRate = 0.15;
    private static final double priceRate = 0.1;

    // 定义各个因素的权重文件路径
    private static final String c1Path= "D:\\PM\\idea-pm\\sensitive-words-realtime-dev-liubo2\\docs\\txt/c1Weight.txt";
    private static final String pricePath= "D:\\PM\\idea-pm\\sensitive-words-realtime-dev-liubo2\\docs\\txt/priceWeight.txt";
    private static final String timePath= "D:\\PM\\idea-pm\\sensitive-words-realtime-dev-liubo2\\docs\\txt/timeWeight.txt";
    private static final String tmPath= "D:\\PM\\idea-pm\\sensitive-words-realtime-dev-liubo2\\docs\\txt/tmWeight.txt";

    // 定义存储各个因素权重的HashMap
    private static  HashMap<String,JSONObject> c1Map ;
    private static  HashMap<String,JSONObject> tmMap ;
    private static  HashMap<String,JSONObject> timeMap ;
    private static  HashMap<String,JSONObject> priceMap ;

    // 静态代码块，用于初始化权重Map
    static {
        c1Map = ReadToJson.readFileToJsonMap(c1Path);
        tmMap = ReadToJson.readFileToJsonMap(tmPath);
        timeMap = ReadToJson.readFileToJsonMap(timePath);
        priceMap = ReadToJson.readFileToJsonMap(pricePath);
    }


    @Override
    public JSONObject map(JSONObject value) throws Exception {
        //时间数据
        String timeType = value.getString("time_type");
        JSONObject timeObj = timeMap.get(timeType);
        value.put("time_code", timeObj);

        //价格区间
        String priceLevel = value.getString("price_level");
        JSONObject priceObj = priceMap.get(priceLevel);
        value.put("price_code", priceObj);

        //类目
        String c1 = value.getString("c1_name");
        JSONObject c1Obj = c1Map.get(c1);
        value.put("c1_code", c1Obj);

        //品牌
        String tmName = value.getString("tm_name");
        JSONObject tmObj = tmMap.get(tmName);
        if (tmObj!=null){
            value.put("tm_code", tmObj);
        }else {
            value.put("tm_code", tmMap.get("香奈儿"));
        }


        return value;
    }
}
