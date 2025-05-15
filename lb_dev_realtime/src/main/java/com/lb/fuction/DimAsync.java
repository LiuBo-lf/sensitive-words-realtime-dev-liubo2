package com.lb.fuction;

import com.alibaba.fastjson.JSONObject;
import com.lb.utils.HbaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
   异步io关联
 */
public abstract class DimAsync<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    private AsyncConnection hbaseCon;


    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseCon= HbaseUtil.getHbaseAsyncCon();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHbaseAsyncCon(hbaseCon);
    }
    @Override
    public void asyncInvoke(T data, ResultFuture<T> resultFuture) throws Exception {

        CompletableFuture.supplyAsync(()->{
            //通过抽象方法获取rowKey
            String rowKey = getRowKey(data);
            JSONObject dimAsync = HbaseUtil.readDimAsync(hbaseCon, "realtime_v1", getTableName(), rowKey);
//            System.out.println("aaaaaaaaaa"+dimAsync);
            return dimAsync;
        }).thenAccept(dimAsync -> {

            if (dimAsync!=null){

                //抽象方法添加字段
                addDims(data, dimAsync);

                //处理后的数据传入下游
                resultFuture.complete(Collections.singleton(data));

            }else {
                System.out.println("未查询到dim维度数据，关联失败");
            }
        });
    }
}
