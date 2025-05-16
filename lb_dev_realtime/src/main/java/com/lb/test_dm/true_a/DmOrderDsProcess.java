package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.utils.CheckPointUtils;
import com.lb.utils.DateFormatUtil;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DmOrderDsProcess {
    @SneakyThrows
    public static void main(String[] args) {
        //初始化流处理环境并设置检查点。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "HeightWeightCDC2Kafka1");
        //从 CDC 源读取用户信息数据流。
        DataStreamSource<String> cdc = SourceSinkUtils.cdcRead(env, "realtime_v1", "user_info_sup_msg");
//        cdc.print();
        SingleOutputStreamOperator<JSONObject> weightHeightDs = cdc.map(o -> JSON.parseObject(o));
        //从 Kafka 源读取用户和订单数据流。
        DataStreamSource<String> userAndOdDs = SourceSinkUtils.kafkaRead(env, "od_join_user");
        //将用户和订单数据流转换为 JSON 对象。
        SingleOutputStreamOperator<JSONObject> mapUserAndOdDs = userAndOdDs.map(o -> JSON.parseObject(o));

        //去重
//        SingleOutputStreamOperator<JSONObject> distinctDs = userAndOdDs
//                .keyBy(o -> JSON.parseObject(o).getString("user_id"))
//                .process(new KeyedProcessFunction<String, String, JSONObject>() {
//                    ValueState<JSONObject> userState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>("userState", JSONObject.class);
//                        userState = getRuntimeContext().getState(descriptor);
//                    }
//
//                    @Override
//                    public void processElement(String value, KeyedProcessFunction<String, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                        try {
//                            JSONObject object = JSON.parseObject(value);
//                            JSONObject stateData = userState.value();
//
//
//                            if (stateData == null) {
//                                out.collect(object);
//                            }
//
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        userState.update(JSON.parseObject(value));
//                    }
//                });
        //weightHeight数据设置水位线
        SingleOutputStreamOperator<JSONObject> waterWeightHeightDs = weightHeightDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        //od_user数据设置水位线
        SingleOutputStreamOperator<JSONObject> odUserDs = mapUserAndOdDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));

        //将 od_user 和 weightHeight 数据流进行区间连接，合并用户信息和体重高度信息。
        SingleOutputStreamOperator<JSONObject> od_user_weightDs = odUserDs
                .keyBy(o -> o.getString("user_id"))
                .intervalJoin(waterWeightHeightDs.keyBy(o -> o.getJSONObject("after").getString("uid")))
                .between(Time.days(-50), Time.days(50))
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

//        od_user_weightDs.print("od_user_weightDs==>");

        SingleOutputStreamOperator<JSONObject> baseDs = od_user_weightDs
//                    .print();
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //时间处理 时间类型
                        Long createTime = value.getLong("create_time");
                        String hourStr = DateFormatUtil.tsToDateTime(createTime).split(" ")[1].split(":")[0];
                        int hour = new Integer(hourStr);
                        getTimeType(value, hour);

                        Long userBirthday = value.getLong("user_birthday");
                        LocalDate birthdayDate = LocalDate.ofEpochDay(userBirthday);
                        String birthday = birthdayDate.format(DateTimeFormatter.ISO_DATE);

                        //价格区间处理 split_total_amount
                        Double price = value.getDouble("split_total_amount");
                        if (price<=1000){
                            value.put("price_level", "低价商品");
                        }else if (price>1000&&price<=3000){
                            value.put("price_level", "中价商品");
                        }else if (price>3000){
                            value.put("price_level", "高价商品");
                        }

                        //c1处理
                        String c1 = value.getString("category1_name");
                        if("个护化妆".equals(c1)){
                            value.put("c1_name", "潮流服饰");
                        } else if ("食品饮料、保健食品".equals(c1) ) {
                            value.put("c1_name", "健康食品");
                        }else {
                            value.put("c1_name", "家居用品");
                        }

                        //生日
                        value.put("birthday", birthday);
                        //年龄
                        value.put("age", getAge(birthdayDate));
                        //星座
                        value.put("starSign", getStarSign(birthdayDate));
                        int i = new Integer(birthday.substring(0, 4)) / 10 *10;
                        value.put("decade", i);


                        return value;
                    }
                });
        //{"birthday":"1983-03-09","starSign":"双鱼座","decade":1980,"category2_name":"手机通讯","time_type":"凌晨","sku_num":"1","spu_name":"Apple iPhone 12","user_name":"韩群豪","split_original_amount":"8197.0000","date_id":"2025-05-14","tm_name":"苹果","unit_height":"cm","category1_name":"手机","sku_name":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机","id":"576","spu_id":"3","category2_id":"13","height":"185","create_time":"1747170502000","split_coupon_amount":"0.0","weight":"56","sku_id":"8","category1_id":"2","user_birthday":"4815","user_login_name":"y60z4z0oye","tm_id":"2","user_id":"147","province_id":"11","price_level":"高价商品","category3_name":"手机","unit_weight":"kg","order_id":"316","category3_id":"61","split_activity_amount":"0.0","ts_ms":"1747032723000","age":42,"split_total_amount":"8197.0"}
        //加权重处理
//        baseDs.print();
        SingleOutputStreamOperator<JSONObject> codeOrderDs = baseDs.map(new OrderGradeMap());
//        codeOrderDs.print();
        SingleOutputStreamOperator<JSONObject> OrderFinalDs = codeOrderDs
                .keyBy(data -> data.getString("user_id"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
        //将最终订单数据流输出到 Kafka。
        OrderFinalDs.print();
        OrderFinalDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("dm_order_final_v2"));

        //禁用操作符链并启动流处理环境。
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


    public static Integer getAge(LocalDate date){

        //获取当前日期。
        LocalDate localDate = LocalDate.now();
        //计算输入日期与当前日期之间的间隔。
        Period between = Period.between(date, localDate);
        //返回间隔的年数作为年龄。
        return between.getYears();
    }



    public static void getTimeType(JSONObject value , Integer hour) {
        //根据小时数判断时间段，并将结果存入 JSON 对象中。
        if (hour>=0&&hour<6){
            value.put("time_type", "凌晨");
        }else if (hour>=6&&hour<9){
            value.put("time_type", "早晨");
        }else if (hour>=9&&hour<12){
            value.put("time_type", "上午");
        }else if (hour>=12&&hour<14){
            value.put("time_type", "中午");
        }else if (hour>=14&&hour<18){
            value.put("time_type", "下午");
            //继续判断其他时间段。
        }else if (hour>=18&&hour<22){
            value.put("time_type", "晚上");
            //如果小时数在 18 到 22 之间，将时间段设为“晚上”。
        }else {
            //如果小时数不在上述任何时间段内，将时间段设为“夜间”。
            value.put("time_type", "夜间");
        }

    }


}
