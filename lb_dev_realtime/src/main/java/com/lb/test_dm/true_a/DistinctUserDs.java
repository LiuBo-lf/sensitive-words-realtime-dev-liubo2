package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * DistinctUserDs类继承自KeyedProcessFunction，用于处理 keyed 流的数据
 * 主要功能包括：计算用户独立访问数（PV）、收集特定字段的不重复值
 */
public class DistinctUserDs extends KeyedProcessFunction<String, JSONObject,JSONObject> {

    // 用于存储页面访问数（PV）的状态
    private transient ValueState<Long> pvState;
    // 用于存储字段不重复值的映射状态
    private transient MapState<String, Set<String>> fieldsState;

    /**
     * 初始化状态
     * @param parameters 配置参数
     * @throws Exception 如果初始化失败
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV状态
        pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv-state", Long.class)
        );

        // 初始化字段集合状态（使用TypeHint保留泛型信息）
        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                new MapStateDescriptor<>(
                        "fields-state",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Set<String>>() {})
                );

        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor);
    }

    /**
     * 更新字段集合
     * @param field 字段名
     * @param value 字段值
     * @throws Exception 如果更新失败
     */
    private void updateField(String field, String value) throws Exception {
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        set.add(value);
        fieldsState.put(field, set);
    }

    /**
     * 获取字段集合
     * @param field 字段名
     * @return 字段值的集合
     * @throws Exception 如果获取失败
     */
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }

    /**
     * 处理每个元素
     * @param value 输入的JSON对象
     * @param ctx 上下文
     * @param out 用于输出的收集器
     * @throws Exception 如果处理失败
     */
    @Override
    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 更新PV
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv);

        // 提取设备信息和搜索关键词
        JSONObject deviceInfo = value.getJSONObject("log_common_info");
        String os = deviceInfo.getString("os");
        String ch = deviceInfo.getString("ch");
        String md = deviceInfo.getString("md");
        String ba = deviceInfo.getString("ba");
        String searchItem = value.containsKey("keyword") ? value.getString("keyword") : null;

        // 更新字段集合
        updateField("os", os.split(" ")[0]);
        updateField("ch", ch);
        updateField("md", md);
        updateField("ba", ba);
        if (searchItem != null) {
            updateField("keyword", searchItem);
        }

        // 构建输出JSON
        JSONObject output = new JSONObject();
        output.put("uid", value.getString("uid"));
        output.put("pv", pv);
        output.put("ts", value.getString("ts"));
        output.put("os", String.join(",", getField("os")));
        output.put("ch", String.join(",", getField("ch")));
        output.put("md", String.join(",", getField("md")));
        output.put("ba", String.join(",", getField("ba")));
        output.put("keyword", String.join(",", getField("keyword")));

        // 输出处理后的JSON对象
        out.collect(output);
    }
}

