package com.lb.test_dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lb.fuction.DimAsync;
import com.lb.test_dm.utils.FilterBloomDeduplicatorFunc;
import com.lb.utils.HbaseUtil;
import com.lb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DwsSkuOrderDm {
    @SneakyThrows
    public static void main(String[] args) {
        //初始化流执行环境并设置并行度和checkpoint配置。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/DwsSkuOrder2");
        //从Kafka读取订单详情数据并进行处理。
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "dwd_trade_order_detail_v1");
//        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> process = kafkaRead.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {


                try {
                    out.collect(JSON.parseObject(value));
                } catch (Exception e) {
                    System.out.println("有脏数据");
                }


            }
        });
        //对处理后的数据进行去重处理。
        KeyedStream<JSONObject, String> keyedStream = process.keyBy(o -> o.getString("id"));
//        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> distinctDs = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("dsfsdf", JSONObject.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject data = valueState.value();
                if (data != null) {
                    String splitOriginalAmount = data.getString("split_original_amount");
                    String splitCouponAmount = data.getString("split_coupon_amount");
                    String splitActivityAmount = data.getString("split_activity_amount");
                    String splitTotalAmount = data.getString("split_total_amount");

                    data.put("split_original_amount", "-" + splitOriginalAmount);
                    data.put("split_coupon_amount", "-" + splitCouponAmount);
                    data.put("split_activity_amount", "-" + splitActivityAmount);
                    data.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(data);
                }
                valueState.update(value);
                out.collect(value);

            }
        });
        SingleOutputStreamOperator<JSONObject> filterDs = distinctDs.keyBy(o->o.getString("user_id")).filter(new FilterBloomDeduplicatorFunc(1000000, 0.01, "user_id", "ts_ms"));


        //todo 异步连接sku_id
        DataStream<JSONObject> resultStream =
                AsyncDataStream.unorderedWait(filterDs,
                        //如何发送异步请求
                        new RichAsyncFunction<JSONObject, JSONObject>() {
                            private AsyncConnection hbaseCon;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseCon = HbaseUtil.getHbaseAsyncCon();
                            }

                            @Override
                            public void close() throws Exception {
                                HbaseUtil.closeHbaseAsyncCon(hbaseCon);
                            }

                            @Override
                            public void asyncInvoke(JSONObject data, ResultFuture<JSONObject> resultFuture) throws Exception {
                                String skuId = data.getString("sku_id");
                                JSONObject dimAsync = HbaseUtil.readDimAsync(hbaseCon, "realtime_v1", "dim_sku_info", skuId);
                                if (dimAsync != null) {
                                    data.put("sku_name", dimAsync.getString("sku_name"));
                                    data.put("spu_id", dimAsync.getString("spu_id"));
                                    data.put("category3_id", dimAsync.getString("category3_id"));
                                    data.put("tm_id", dimAsync.getString("tm_id"));
                                    //处理后的数据传入下游
                                    resultFuture.complete(Collections.singleton(data));

                                } else {
                                    System.out.println("未查询到dim维度数据，关联失败");
//                                    System.out.println("aaaaaa"+skuId);
//                                    System.out.println("aaaaaaa"+dimAsync.toJSONString());
                                }

                            }
                        },
                        200, TimeUnit.SECONDS);
//        resultStream.print();

        //todo 异步连接spu_id
        SingleOutputStreamOperator<JSONObject> spu_connect = AsyncDataStream.unorderedWait(resultStream,
                new DimAsync<JSONObject>() {
                    @Override
                    public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                        String spuName = dimJsonObj.getString("spu_name");
                        obj.put("spu_name", spuName);
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(JSONObject obj) {
                        return obj.getString("spu_id");
                    }
                },
                100, TimeUnit.SECONDS);
//spu_connect.print();
        // todo 连接c3
        SingleOutputStreamOperator<JSONObject> c3_connect = AsyncDataStream.unorderedWait(spu_connect, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                obj.put("category3_name", dimJsonObj.getString("name"));
                obj.put("category2_id", dimJsonObj.getString("category2_id"));
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getString("category3_id");
            }
        }, 60, TimeUnit.SECONDS);
//        c3_connect.print();

        //连接c2
        SingleOutputStreamOperator<JSONObject> c2_connect = AsyncDataStream.unorderedWait(c3_connect, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                obj.put("category2_name", dimJsonObj.getString("name"));
                obj.put("category1_id", dimJsonObj.getString("category1_id"));
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getString("category2_id");
            }
        }, 60, TimeUnit.SECONDS);


        //连接c1
        SingleOutputStreamOperator<JSONObject> c1_connect = AsyncDataStream.unorderedWait(c2_connect, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                obj.put("category1_name", dimJsonObj.getString("name"));

            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getString("category1_id");
            }
        }, 60, TimeUnit.SECONDS);

//连接c1
        SingleOutputStreamOperator<JSONObject> tm_connect = AsyncDataStream.unorderedWait(c1_connect, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                obj.put("tm_name", dimJsonObj.getString("tm_name"));

            }

            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getString("tm_id");
            }
        }, 60, TimeUnit.SECONDS);

        //打印最终处理结果并将其写入Kafka。
        tm_connect.print();
        tm_connect.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("dws_sku_order_detail_v1"));
        env.disableOperatorChaining();
        env.execute();
    }



}
