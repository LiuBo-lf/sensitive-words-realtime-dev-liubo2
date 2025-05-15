package com.lb.test_dm.true_a;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DmFinalJoinCode {
    private static final double keyWordRate = 0.15;
    private static final double deviceRate = 0.1;
    private static final double c1Rate = 0.3;
    private static final double tmRate = 0.2;
    private static final double timeRate = 0.15;
    private static final double priceRate = 0.1;
    private static final List<String> rank = new ArrayList<>();
    static {
        rank.add("18-24");
        rank.add("25-29");
        rank.add("30-34");
        rank.add("35-39");
        rank.add("40-49");
        rank.add("50以上");

    }
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dmOrderDs = SourceSinkUtils.kafkaRead(env, "dm_order_final_v2");
        SingleOutputStreamOperator<JSONObject> dmOrderJsonDs= dmOrderDs.map(o -> JSON.parseObject(o));
        DataStreamSource<String> dmKeywordDs = SourceSinkUtils.kafkaReadSetWater(env, "dm_keyword_final");
        SingleOutputStreamOperator<JSONObject> dmKeywordJsonDs = dmKeywordDs.map(o -> JSON.parseObject(o));
        dmOrderJsonDs
                .keyBy(o -> o.getString("user_id"))
                .intervalJoin(dmKeywordJsonDs.keyBy(o -> o.getString("uid")))
                .between(Time.days(-50), Time.days(50))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        JSONObject result = new JSONObject();
                        //左边取值
                        String starSign = left.getString("starSign");
                        String decade = left.getString("decade");
                        String age = left.getString("age");
                        String unit_weight = left.getString("unit_weight");
                        String weight = left.getString("weight");
                        String unit_height = left.getString("unit_height");
                        String height = left.getString("height");
                        String sex = left.getString("sex");
                        String user_id = left.getString("user_id");

                        result.put("starSign", starSign);
                        result.put("decade", decade);
                        result.put("age", age);
                        result.put("unit_weight", unit_weight);
                        result.put("weight", weight);
                        result.put("unit_height", unit_height);
                        result.put("height", height);
                        result.put("sex", sex);
                        result.put("user_id", user_id);


                        JSONObject tm_code = left.getJSONObject("tm_code");
                        JSONObject time_code = left.getJSONObject("time_code");
                        JSONObject price_code = left.getJSONObject("price_code");
                        JSONObject c1_code = left.getJSONObject("c1_code");
                        //右边
                        JSONObject  device_code = right.getJSONObject("device_weight");
                        JSONObject  keyword_code = right.getJSONObject("keyword_weight");
                        //推测年龄

                        String inferredAge = getInferredAge(c1_code, tm_code, time_code, price_code, device_code, keyword_code);
                        result.put("inferredAge", inferredAge);
                        out.collect(result);

                    }
                }).print();


        env.disableOperatorChaining();
        env.execute();
    }
    public static String getInferredAge(JSONObject c1 ,JSONObject tm,JSONObject time ,JSONObject price,JSONObject device,JSONObject keyword) {

        ArrayList<Double> rankCods = new ArrayList<Double>();
        for (String s : rank) {

            try{
                double v = c1.getDouble(s) * c1Rate
                        + tm.getDouble(s) * tmRate
                        + time.getDouble(s) * timeRate
                        + price.getDouble(s) * priceRate
                        + device.getDouble(s) * deviceRate
                        + keyword.getDouble(s) * keyWordRate;
                rankCods.add(v);
            }catch (Exception e){

                System.err.println("time"+time);
                System.err.println("c"+c1);
                System.err.println("t"+tm);
                System.err.println("p"+price);
                System.err.println("d"+device);
                System.err.println("k"+keyword);
            }
        }
        rankCods.add(0.0);
        double maxValue2 = Collections.max(rankCods);
        int index2 = rankCods.indexOf(maxValue2);
        return rank.get(index2);

    }
}
