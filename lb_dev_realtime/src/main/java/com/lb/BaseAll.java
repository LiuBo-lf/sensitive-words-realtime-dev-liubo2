package com.lb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60*1000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/stream_dev_v1/checkpoint");
        env.disableOperatorChaining();
    }
}