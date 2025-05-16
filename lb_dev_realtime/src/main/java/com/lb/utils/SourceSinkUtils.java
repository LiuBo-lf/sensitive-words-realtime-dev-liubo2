package com.lb.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class SourceSinkUtils {

    public static DataStreamSource<String> cdcRead(StreamExecutionEnvironment env, String dbName, String tableName) throws Exception {

        //设置并行度为1，以确保数据流处理的顺序性。
        env.setParallelism(1);
        //初始化并配置Properties对象，用于处理Decimal和时间精度。
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode", "connect");

        //构建MySqlSource对象，配置MySQL连接和数据捕获选项，包括主机名、端口、数据库、表、用户名、密码、启动选项和反序列化器。
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList(dbName) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(dbName + "." + tableName) // 设置捕获的表
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.earliest())
//                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        //将构建的MySqlSource对象转换为DataStreamSource对象，并设置水印策略和源名称。
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //返回构建的DataStreamSource对象。
        return mySQLSource;
    }



    public static KafkaSink<String> sinkToKafka(String topic_name) {
        //使用KafkaSink.Builder构建KafkaSink实例。
        KafkaSink<String> sink = KafkaSink.<String>builder()
                //设置Kafka集群的Bootstrap服务器地址。
                .setBootstrapServers("cdh03:9092")
                //配置记录序列化器，指定主题和值序列化方式。
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic_name)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //设置消息传递保证为至少一次。
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        //返回构建好的KafkaSink实例。
        return sink;


    }


    public static DataStreamSource<String>  kafkaRead(StreamExecutionEnvironment env,String topic ){

        //构建Kafka数据源，设置服务器、主题、消费者组ID、起始偏移量和反序列化器。
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics(topic)
                .setGroupId(topic+"1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //从构建的Kafka数据源创建DataStreamSource，并设置无水印策略。
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //返回创建的Kafka数据源。
        return kafkaSource;
    }


    public static DataStreamSource<String>  kafkaReadSetWater(StreamExecutionEnvironment env,String topic ){

        //创建Kafka数据源，配置服务器地址、主题、组ID、起始偏移量和反序列化器。
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics(topic)
                .setGroupId(topic+"1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //从Kafka数据源创建DataStreamSource，并设置水印策略以处理乱序事件。
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> {
                            //检查事件是否为空，如果非空则尝试解析JSON对象。
                            if (event != null){
                                try {
                                    JSONObject object = JSON.parseObject(event);

                                    //从JSON对象中提取时间戳，优先使用"tm_ms"字段，如果不存在则使用"ts"字段。
                                    if (object.containsKey("tm_ms")&&object.getString("ts_ms")!=null){
                                        return object.getLong("ts_ms");
                                    }else if (object.containsKey("ts")&&object.getString("ts")!=null){
                                        return object.getLong("ts");
                                    }

                                    //捕获解析JSON对象时的异常，并输出错误信息，返回默认时间戳0L。
                                }catch (Exception e){
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    return 0L;
                                }
                            }
                            return 0L;
                        }
                ), "Kafka Source");
        return kafkaSource;
    }


    public static DorisSink<String> getDorisSink(String db,String tableName){
        //初始化属性配置，设置数据格式为 JSON。
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        //构建 DorisSink 对象，设置读取选项。
        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("cdh03:8030")
                        .setTableIdentifier( db+ "." + tableName)
                        .setUsername("root")
                        .setPassword("root")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                //设置序列化器并构建 DorisSink 对象。
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }


}
