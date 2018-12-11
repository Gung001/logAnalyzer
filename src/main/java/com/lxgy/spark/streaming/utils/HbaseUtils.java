package com.lxgy.spark.streaming.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Gryant
 */
public class HbaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HbaseUtils() {

        // 添加配置信息
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "data01:2181");
        configuration.set("hbase.rootdir", "hdfs://data01:8020/hbase");

        // hbase 操作对象
        try {

            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private volatile static HbaseUtils instance = null;

    /**
     * 获取实例
     *
     * @return
     */
    public static HbaseUtils newInstance() {
        if (instance == null) {
            synchronized (HbaseUtils.class) {
                if (instance == null) {
                    instance = new HbaseUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 获取表实例
     *
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {

        HTable hTable = null;

        try {

            hTable = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hTable;
    }

    /**
     * 添加一条记录到HBase表
     *
     * @param tableName 表名
     * @param rowKey
     * @param cf        列簇
     * @param column    列
     * @param value     值
     */
    public void put(String tableName, String rowKey, String cf, String column, String value) {

        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

//        HTable table = HbaseUtils.newInstance().getTable("course_click_count");

//        System.out.println(table.getName().getNameAsString());

        HbaseUtils.newInstance().put("course_click_count", "20181209_88", "info", "click_count", "2");
        System.out.println("ok...");
    }

}
