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

public class DmKeyWordsDsProcess {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/dws-logs");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaReadSetWater(env, "topic_log");
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
        OutputTag<JSONObject> haveKeyWord = new OutputTag<JSONObject>("haveKeyWord") {
        };
        SingleOutputStreamOperator<JSONObject> withUidDs =
                jsonDs
                        .process(new ProcessFunction<JSONObject, JSONObject>() {

                            @Override
                            public void processElement(JSONObject obj, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject newObj = new JSONObject();
                                if (
                                        obj.containsKey("common")
                                                && (!obj.getJSONObject("common").isEmpty())
                                                && obj.containsKey("page")
                                                && (!obj.getJSONObject("page").isEmpty())
                                ) {

                                    JSONObject common = obj.getJSONObject("common");
                                    JSONObject page = obj.getJSONObject("page");
                                    String os = common.getString("os").split(" ")[0];
                                    newObj.put("os", os);
                                    newObj.put("ts", obj.getString("ts"));
                                    String uid = ((!common.containsKey("uid")) || (common.getString("uid").isEmpty())) ? "-1" : common.getString("uid");
                                    newObj.put("uid", uid);
                                    common.remove("ar");
                                    common.remove("is_new");
                                    common.remove("sid");
                                    newObj.put("log_common_info", common);
                                    newObj.put("keyword", "");
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
//        withUidDs.print();
//        SideOutputDataStream<JSONObject> keyWordDs = withUidDs.getSideOutput(haveKeyWord);
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
//                String jsonString = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue);
                String jsonString = value.toJSONString();
                System.err.println("处理数据中" + jsonString);
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
                SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = distinctDs
                        .keyBy(data -> data.getString("uid"))
                .process(new DistinctUserDs())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

                //加权重处理
        SingleOutputStreamOperator<JSONObject> finalKeyWordDs = win2MinutesPageLogsDs
                .keyBy(o -> o.getString("uid"))
                .map(new KeyWordGradeMap());
        finalKeyWordDs.print();
        finalKeyWordDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("dm_keyword_final"));

        env.disableOperatorChaining();
        env.execute();

    }
}
