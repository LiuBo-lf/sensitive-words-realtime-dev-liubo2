package com.lb.test_dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.utils.CheckPointUtils;
import com.lb.utils.DateFormatUtil;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class Base6Tag {
    @SneakyThrows
    public static void main(String[] args) {
        //初始化流执行环境并设置检查点。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "HeightWeightCDC2Kafka1");
        //从数据库读取用户信息流。
        DataStreamSource<String> cdc = SourceSinkUtils.cdcRead(env, "online_flink_retail", "user_info_sup_msg");
//        cdc.print();
        SingleOutputStreamOperator<JSONObject> weightHeightDs = cdc.map(o -> JSON.parseObject(o));
        DataStreamSource<String> userAndOdDs = SourceSinkUtils.kafkaRead(env, "od_join_user");
        DataStreamSource<String> userKeywordDs = SourceSinkUtils.kafkaRead(env, "user_keyword");

        //将用户关键词流转换为 JSON 对象流。
        SingleOutputStreamOperator<JSONObject> userKeyJsonDs = userKeywordDs.map(o -> JSON.parseObject(o));
        //对用户信息流进行去重处理。
        SingleOutputStreamOperator<JSONObject> distinctDs = userAndOdDs
                .keyBy(o -> JSON.parseObject(o).getString("user_id"))
                .process(new KeyedProcessFunction<String, String, JSONObject>() {
                    ValueState<JSONObject> userState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>("userState", JSONObject.class);
                        userState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject object = JSON.parseObject(value);
                            JSONObject stateData = userState.value();


                            if (stateData == null) {
                                out.collect(object);
                            }

                        } catch (Exception e) {
                            System.err.println(e);
                        }
                        userState.update(JSON.parseObject(value));
                    }
                });
        //weightHeight数据设置水位线
        SingleOutputStreamOperator<JSONObject> wmWhDs = weightHeightDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        //od_user数据设置水位线
        SingleOutputStreamOperator<JSONObject> wmUserDs = distinctDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        //为用户关键词数据流设置水位线。
        SingleOutputStreamOperator<JSONObject> userKeyWaterDs = userKeyJsonDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
                );

        //intervalJoin 两流
        SingleOutputStreamOperator<JSONObject> od_user_weightDs = wmUserDs
                .keyBy(o -> o.getString("user_id"))
                .intervalJoin(wmWhDs.keyBy(o -> o.getJSONObject("after").getString("uid")))
                .between(Time.days(-2), Time.days(2))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        JSONObject clone = (JSONObject) left.clone();
                        clone.put("weight", right.getJSONObject("after").getString("weight"));
//                        clone.put("gender", right.getJSONObject("after").getString("gender"));
                        clone.put("height", right.getJSONObject("after").getString("height"));
                        clone.put("unit_height", right.getJSONObject("after").getString("unit_height"));
                        clone.put("unit_weight", right.getJSONObject("after").getString("unit_weight"));
                        out.collect(clone);
                    }
                });
        SingleOutputStreamOperator<JSONObject> baseDs = od_user_weightDs
//                    .print();
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {


                        Long userBirthday = value.getLong("user_birthday");
                        LocalDate birthdayDate = LocalDate.ofEpochDay(userBirthday);
                        String birthday = birthdayDate.format(DateTimeFormatter.ISO_DATE);

                        //生日
                        value.put("birthday", birthday);
                        //年龄
                        value.put("age", DateFormatUtil.getAge(birthdayDate));
                        //星座
                        value.put("starSign", getStarSign(birthdayDate));
                        int i = new Integer(birthday.substring(0, 4)) / 10 *10;
                        value.put("decade", i);


                        return value;
                    }
                });
        //打印处理后的数据流，并将其写入 Kafka。
        baseDs.print();
        baseDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("base6Tag"));
//        userKeyWaterDs.print("key==>");
//        SingleOutputStreamOperator<JSONObject> join3Ds = od_user_weightDs
//                .keyBy(o -> o.getString("user_id"))
//                .intervalJoin(userKeyWaterDs.keyBy(o -> o.getString("uid")))
//                .between(Time.days(-2), Time.days(2))
//                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
//                    @Override
//                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                        JSONObject clone = (JSONObject) left.clone();
//                        clone.put("os", right.getString("os"));
//                        clone.put("md", right.getString("md"));
//                        clone.put("vc", right.getString("vc"));
//                        clone.put("ba", right.getString("ba"));
//                    }
//                });
//        join3Ds.print();


        //禁用操作符链并执行流处理环境。
        env.disableOperatorChaining();
        env.execute();


    }


    public static String getStarSign(LocalDate birthday) {
        //获取生日的月份和日期。
        int month = birthday.getMonthValue();
        int day = birthday.getDayOfMonth();

        // 使用Map存储星座及其对应的日期区间
        Map<String, int[]> zodiacRanges = new HashMap<>();
        zodiacRanges.put("白羊座", new int[]{3, 21, 4, 19});
        zodiacRanges.put("金牛座", new int[]{4, 20, 5, 20});
        zodiacRanges.put("双子座", new int[]{5, 21, 6, 21});
        zodiacRanges.put("巨蟹座", new int[]{6, 22, 7, 22});
        zodiacRanges.put("狮子座", new int[]{7, 23, 8, 22});
        zodiacRanges.put("处女座", new int[]{8, 23, 9, 22});
        zodiacRanges.put("天秤座", new int[]{9, 23, 10, 23});
        zodiacRanges.put("天蝎座", new int[]{10, 24, 11, 22});
        zodiacRanges.put("射手座", new int[]{11, 23, 12, 21});
        zodiacRanges.put("摩羯座", new int[]{12, 22, 1, 19});
        zodiacRanges.put("水瓶座", new int[]{1, 20, 2, 18});
        zodiacRanges.put("双鱼座", new int[]{2, 19, 3, 20});

        //遍历Map，根据生日的月份和日期匹配对应的星座。
        for (Map.Entry<String, int[]> entry : zodiacRanges.entrySet()) {
            int[] range = entry.getValue();
            if ((month == range[0] && day >= range[1]) || (month == range[2] && day <= range[3])) {
                return entry.getKey();
            }
        }
        //如果没有匹配到任何星座，返回“未知”。
        return "未知";
    }


}
