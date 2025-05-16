package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;

public class KeyWordGradeMap  extends RichMapFunction<JSONObject,JSONObject> {


    // 定义关键字权重文件的路径
    private static final String keyWordPath= "D:\\PM\\idea-pm\\bj_i18_teams\\docs\\txt/keyWordWeight.txt";
    // 定义设备权重文件的路径
    private static final String devicePath= "D:\\PM\\idea-pm\\bj_i18_teams\\docs\\txt/deviceWeight.txt";

    // 存储关键字及其对应权重的映射
    private static HashMap<String,JSONObject> keyWordMap ;
    // 存储设备及其对应权重的映射
    private static  HashMap<String,JSONObject> deviceMap ;

    // 静态代码块，用于初始化关键字和设备的权重映射
    static {
        // 读取关键字权重文件，并将内容转换为JSON对象的映射
        keyWordMap = ReadToJson.readFileToJsonMap(keyWordPath);
        // 读取设备权重文件，并将内容转换为JSON对象的映射
        deviceMap = ReadToJson.readFileToJsonMap(devicePath);
    }


    @Override
    public JSONObject map(JSONObject value) throws Exception  {

        //从输入的 JSON 对象中获取关键词，并根据关键词设置搜索类别。
        String keyword = value.getString("keyword");
        if (keyword.contains("匡威")){
            value.put("search", "时尚与潮流");
        }else if (keyword.contains("衬衫") || keyword.contains("心相印纸抽")){
            value.put("search", "性价比");
        } else if (keyword.contains("扫地机器人")) {
            value.put("search", "家庭与育儿");
        }else if (keyword.contains("拯救者")|| keyword.contains("小米")){
            value.put("search", "数码与科技");
        }else if (keyword.contains("联想")){
            value.put("search", "学习与发展");
        }else {
            value.put("search", "健康与养生");
        }
        //搜索关键词
        String search = value.getString("search");
        JSONObject keywordObj = keyWordMap.get(search);
        value.put("keyword_weight", keywordObj);

        //从输入的 JSON 对象中获取操作系统信息，提取第一个操作系统名称，并获取相应的设备权重，将其添加到 JSON 对象中。
        String device_os = value.getString("os").split(",")[0];
        value.put("device_os",device_os);
        JSONObject deviceObj = deviceMap.get(device_os);
        value.put("device_weight", deviceObj);
        return value;




    }


}
