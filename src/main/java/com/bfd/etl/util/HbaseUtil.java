package com.bfd.etl.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName HbaseUtil
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/16 18:01
 * @Version 1.0
 **/
public class HbaseUtil {

    private static Logger logger = Logger.getLogger(HbaseUtil.class);
    private static final String CONNECTION_KEY = "hbase.zookeeper.quorum";
    private static final String CONNECTION_VALUE = "hadoop01:2181";

    private static final TableName TABLE_NAME = TableName.valueOf("event_logs");

    private static Connection conn = null;

    static{
        Configuration conf = HBaseConfiguration.create();
        conf.set(CONNECTION_KEY, CONNECTION_VALUE);

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.warn("连接到hbase失败");
        }
    }

    public static Admin getAdmin() {
        Admin admin = null;

        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            logger.warn("获取admin失败");
        }

        return admin;
    }

    public static void createTable() {
        Admin admin = getAdmin();
        try {
            if (admin.tableExists(TABLE_NAME)){
                logger.warn("该表已经存在");
                return;
            }else {
                HTableDescriptor ht = new HTableDescriptor(TABLE_NAME);
                HColumnDescriptor hc = new HColumnDescriptor("info");
                ht.addFamily(hc);
                admin.createTable(ht);
            }
        } catch (IOException e) {
            logger.warn("检测失败");
        }
    }


}
