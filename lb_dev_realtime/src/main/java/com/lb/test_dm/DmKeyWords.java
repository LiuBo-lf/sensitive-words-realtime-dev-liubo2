package com.lb.test_dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.test_dm.utils.FilterBloomDeduplicatorFunc;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DmKeyWords {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/dws-logs");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaReadSetWater(env, "log_topic_flink_online_v1_log");
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
                                    newObj.put("log_info", common);
                                    newObj.put("keyword", "性价比");
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


        SingleOutputStreamOperator<JSONObject> processDs = withUidDs.keyBy(o -> o.getString("uid"))
                .window(TumblingEventTimeWindows
                        .of(Time.days(1)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                           ValueState<Long> tsState;

                           @Override
                           public void open(Configuration parameters) throws Exception {
                               ValueStateDescriptor<Long> descriptor =
                                       new ValueStateDescriptor<Long>(
                                               "state", Long.class); // default value of the state, if nothing was set
                               tsState = getRuntimeContext().getState(descriptor);
                           }

                           @Override
                           public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                               JSONObject obj = elements.iterator().next();
                               Long lastTs = tsState.value();
                               Long ts = obj.getLong("ts");
                               if (lastTs != null) {
                                   if (lastTs <= ts) {
                                       tsState.update(ts);
                                       out.collect(obj);
                                   }
                               }
                               tsState.update(ts);
                           }
                       }
                );
        SingleOutputStreamOperator<JSONObject> bloomFilterDs = processDs.keyBy(o->o.getString("uid")).filter(new FilterBloomDeduplicatorFunc(1000000, 0.0001, "uid", "ts"));
        bloomFilterDs.print();


        env.disableOperatorChaining();
        env.execute();

    }
}
