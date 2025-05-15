package com.lb.ods;

import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CdcSinkToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> allData = SourceSinkUtils.cdcRead(env, "realtime_v1", "*");
        allData.print();
        allData.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v1"));
        env.execute("aaaaaaaa");

    }
}
