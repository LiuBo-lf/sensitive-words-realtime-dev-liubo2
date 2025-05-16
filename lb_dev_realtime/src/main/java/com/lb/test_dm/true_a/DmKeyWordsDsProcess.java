package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashSet;

/**
 * 处理数据流中的关键词信息
 */
public class DmKeyWordsDsProcess {
    @SneakyThrows
    public static void main(String[] args) {
        // 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 启用检查点机制
        env.enableCheckpointing(3000);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 配置检查点存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/dws-logs");
        // 设置Hadoop用户名
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        // 从Kafka读取数据
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaReadSetWater(env, "topic_log");
        // 将读取的数据转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                try {
                    return JSON.parseObject(value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
        // 定义一个输出标签，用于标记含有关键词的数据
        OutputTag<JSONObject> haveKeyWord = new OutputTag<JSONObject>("haveKeyWord") {
        };
        // 处理数据，提取关键词信息
        SingleOutputStreamOperator<JSONObject> withUidDs =
                jsonDs
                        .process(new ProcessFunction<JSONObject, JSONObject>() {

                            @Override
                            public void processElement(JSONObject obj, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject newObj = new JSONObject();
                                // 检查数据是否包含必要的字段
                                if (
                                        obj.containsKey("common")
                                                && (!obj.getJSONObject("common").isEmpty())
                                                && obj.containsKey("page")
                                                && (!obj.getJSONObject("page").isEmpty())
                                ) {

                                    JSONObject common = obj.getJSONObject("common");
                                    JSONObject page = obj.getJSONObject("page");
                                    // 提取操作系统信息
                                    String os = common.getString("os").split(" ")[0];
                                    newObj.put("os", os);
                                    newObj.put("ts", obj.getString("ts"));
                                    // 提取用户ID
                                    String uid = ((!common.containsKey("uid")) || (common.getString("uid").isEmpty())) ? "-1" : common.getString("uid");
                                    newObj.put("uid", uid);
                                    // 移除不需要的字段
                                    common.remove("ar");
                                    common.remove("is_new");
                                    common.remove("sid");
                                    newObj.put("log_common_info", common);
                                    newObj.put("keyword", "");
                                    // 检查是否包含关键词
                                    if (obj.getJSONObject("page").containsKey("item_type")
                                            && ("keyword").equals(obj.getJSONObject("page").getString("item_type"))) {
                                        String item = page.getString("item");
                                        newObj.put("keyword", item);
                                        ctx.output(haveKeyWord, newObj);
                                    }
                                    out.collect(newObj);
                                }
                            }
                        });
        // 执行侧输出流
//        withUidDs.print();
//        SideOutputDataStream<JSONObject> keyWordDs = withUidDs.getSideOutput(haveKeyWord);
        // 去重处理
        SingleOutputStreamOperator<JSONObject> distinctDs = withUidDs.keyBy(o->o.getString("uid")).process(new KeyedProcessFunction<String,JSONObject, JSONObject>() {

            ValueState<HashSet<String>> strState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<HashSet<String>> descriptor =
                        new ValueStateDescriptor<HashSet<String>>(
                                "state",
                                TypeInformation.of(new TypeHint<HashSet<String>>() {
                                })
                        );
                strState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                HashSet<String> set = strState.value();
                if (set == null) {
                    set = new HashSet<>();
                }
                // 将JSON对象转换为字符串
                String jsonString = value.toJSONString();
                System.err.println("处理数据中" + jsonString);
                // 检查数据是否已存在
                if (!set.contains(jsonString)) {
                    System.err.println("添加新数据");
                    set.add(jsonString);
                    strState.update(set);
                    out.collect(value);
                } else {
                    System.err.println("数据已存在" + jsonString);
                }


            }
        });
//distinctDs.print();
        // 窗口操作，按用户ID分组，每2分钟一个窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = distinctDs
                .keyBy(data -> data.getString("uid"))
                .process(new DistinctUserDs())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

        // 加权重处理
        SingleOutputStreamOperator<JSONObject> finalKeyWordDs = win2MinutesPageLogsDs
                .keyBy(o -> o.getString("uid"))
                .map(new KeyWordGradeMap());
        finalKeyWordDs.print();
        // 将处理后的数据写入Kafka
        finalKeyWordDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("dm_keyword_final"));

        // 禁用算子链
        env.disableOperatorChaining();
        // 执行任务
        env.execute();

    }
}
