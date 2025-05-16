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
    // 定义各个因素的权重
    private static final double keyWordRate = 0.15;
    private static final double deviceRate = 0.1;
    private static final double c1Rate = 0.3;
    private static final double tmRate = 0.2;
    private static final double timeRate = 0.15;
    private static final double priceRate = 0.1;

    // 定义年龄分组列表
    private static final List<String> rank = new ArrayList<>();

    // 静态代码块，初始化年龄分组
    static {
        rank.add("18-24");
        rank.add("25-29");
        rank.add("30-34");
        rank.add("35-39");
        rank.add("40-49");
        rank.add("50以上");
    }

    // 主函数
    @SneakyThrows
    public static void main(String[] args) {
        // 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从Kafka读取订单数据
        DataStreamSource<String> dmOrderDs = SourceSinkUtils.kafkaRead(env, "dm_order_final_v2");
        // 将订单数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> dmOrderJsonDs= dmOrderDs.map(o -> JSON.parseObject(o));

        // 从Kafka读取关键词数据
        DataStreamSource<String> dmKeywordDs = SourceSinkUtils.kafkaReadSetWater(env, "dm_keyword_final");
        // 将关键词数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> dmKeywordJsonDs = dmKeywordDs.map(o -> JSON.parseObject(o));

        // 对订单数据和关键词数据进行区间连接
        dmOrderJsonDs
                .keyBy(o -> o.getString("user_id"))
                .intervalJoin(dmKeywordJsonDs.keyBy(o -> o.getString("uid")))
                .between(Time.days(-50), Time.days(50))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 创建结果JSON对象
                        JSONObject result = new JSONObject();

                        // 从左边数据中获取用户信息并放入结果对象
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

                        // 从左边数据中获取各类代码对象
                        JSONObject tm_code = left.getJSONObject("tm_code");
                        JSONObject time_code = left.getJSONObject("time_code");
                        JSONObject price_code = left.getJSONObject("price_code");
                        JSONObject c1_code = left.getJSONObject("c1_code");

                        // 从右边数据中获取设备和关键词代码对象
                        JSONObject  device_code = right.getJSONObject("device_weight");
                        JSONObject  keyword_code = right.getJSONObject("keyword_weight");

                        // 推测年龄并放入结果对象
                        String inferredAge = getInferredAge(c1_code, tm_code, time_code, price_code, device_code, keyword_code);
                        result.put("inferredAge", inferredAge);

                        // 输出结果
                        out.collect(result);
                    }
                }).print();

        // 禁用算子链
        env.disableOperatorChaining();
        // 执行环境
        env.execute();
    }

    // 根据各类代码计算推测年龄
    public static String getInferredAge(JSONObject c1 ,JSONObject tm,JSONObject time ,JSONObject price,JSONObject device,JSONObject keyword) {
        // 存储每个年龄分组的计算结果
        ArrayList<Double> rankCods = new ArrayList<Double>();

        // 遍历每个年龄分组，计算权重总和
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
                // 打印错误信息
                System.err.println("time"+time);
                System.err.println("c"+c1);
                System.err.println("t"+tm);
                System.err.println("p"+price);
                System.err.println("d"+device);
                System.err.println("k"+keyword);
            }
        }

        // 添加一个默认值，防止空列表
        rankCods.add(0.0);

        // 找出计算结果中的最大值
        double maxValue2 = Collections.max(rankCods);
        // 获取最大值的索引
        int index2 = rankCods.indexOf(maxValue2);

        // 根据索引返回对应的年龄分组
        return rank.get(index2);
    }
}

