package com.lb.test_dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.utils.CheckPointUtils;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataProcessDm {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "DataProcessDm");
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_v1");

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(o -> JSON.parseObject(o));

        jsonDs.print();
        env.disableOperatorChaining();
        env.execute();
    }
}
