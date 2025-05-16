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
        //获取异步 HBase 连接对象。
        hbaseCon= HbaseUtil.getHbaseAsyncCon();
    }



    @Override
    public void close() throws Exception {
        //关闭 HBase 异步连接。
        HbaseUtil.closeHbaseAsyncCon(hbaseCon);
    }


    @Override
    public void asyncInvoke(T data, ResultFuture<T> resultFuture) throws Exception {

        //使用异步任务从HBase读取数据。
        CompletableFuture.supplyAsync(()->{
            //通过抽象方法获取rowKey
            String rowKey = getRowKey(data);
            JSONObject dimAsync = HbaseUtil.readDimAsync(hbaseCon, "realtime_v1", getTableName(), rowKey);
//            System.out.println("cheshi"+dimAsync);
            return dimAsync;
        }).thenAccept(dimAsync -> {

            //如果读取到的数据不为空，调用抽象方法添加字段并完成结果。
            if (dimAsync!=null){

                //抽象方法添加字段
                addDims(data, dimAsync);

                //处理后的数据传入下游
                resultFuture.complete(Collections.singleton(data));

                //如果未查询到数据，打印关联失败的提示信息。
            }else {
                System.out.println("未查询到dim维度数据，关联失败");
            }
        });
    }


}
