package com.lb.utils;



/**
 * FlinkSQL操作的工具类
 */
public class SQLUtil {
    //获取kafka连接器的连接属性

    /**
     * 从kafka读取
     * @param topic
     * @return
     */
    public static String getKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = 'cdh03:9092',\n" +
                "  'properties.group.id' = '" + topic + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //获取Hbase连接器的连接属性
    public static String getHBaseDDL(String namespace,String tableName) {
        return " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + namespace + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    //获取upsert-kafka连接器的连接属性
    /**
     * 向kafka写入
     * @param topic
     * @return
     */
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + "cdh01:9092,cdh02:9092,cdh03:9092" + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
