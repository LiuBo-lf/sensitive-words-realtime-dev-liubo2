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

public class DistinctUserDs extends KeyedProcessFunction<String, JSONObject,JSONObject> {

    private transient ValueState<Long> pvState;
    private transient MapState<String, Set<String>> fieldsState;


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

    // 辅助方法：更新字段集合
    private void updateField(String field, String value) throws Exception {
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        set.add(value);
        fieldsState.put(field, set);
    }

    // 辅助方法：获取字段集合
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }

    @Override
    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv);
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

        out.collect(output);

    }
}
