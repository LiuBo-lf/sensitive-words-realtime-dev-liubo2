package com.lb.ods;

import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CdcSinkToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        //获取流处理执行环境。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从指定的CDC源读取所有数据。
        DataStreamSource<String> allData = SourceSinkUtils.cdcRead(env, "realtime_v1", "*");
        //打印读取的数据。
        allData.print();
        //将数据写入Kafka主题。
        allData.sinkTo(SourceSinkUtils.sinkToKafka("log_topic_flink_online_v1"));
        //启动流处理任务。
        env.execute("aaaaaaaa");

    }


}
