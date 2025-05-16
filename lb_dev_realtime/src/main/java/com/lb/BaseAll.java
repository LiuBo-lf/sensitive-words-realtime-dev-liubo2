package com.lb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAll {
    public static void main(String[] args) throws Exception {
        //获取当前的执行环境。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1。
        env.setParallelism(1);
        //启用每分钟一次的检查点。
        env.enableCheckpointing(60*1000);
        //配置检查点存储路径。
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/stream_dev_v1/checkpoint");
        //禁用算子链。
        env.disableOperatorChaining();
    }
}