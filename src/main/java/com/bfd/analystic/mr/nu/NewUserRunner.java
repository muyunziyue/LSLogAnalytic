package com.bfd.analystic.mr.nu;

import com.bfd.analystic.model.dim.StatsUserDimension;
import com.bfd.analystic.model.dim.value.map.TimeOutputValue;
import com.bfd.analystic.model.dim.value.reduce.TextOutPutValue;
import com.bfd.analystic.mr.IOutputWriterFormat;
import com.bfd.common.EventLogConstants;
import com.bfd.common.GlobalConstants;
import com.bfd.util.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @ClassName NewUserRunner
 * @Description TODO 新增用户的驱动类
 * @Author Lidexiu
 * @Date 2018/8/21 14:55
 * @Version 1.0
    truncate dimension_browser;
    truncate dimension_currency_type;
    truncate dimension_date;
    truncate dimension_event;
    truncate dimension_inbound;
    truncate dimension_kpi;
    truncate dimension_location;
    truncate dimension_os;
    truncate dimension_payment_type;
    truncate dimension_platform;
    truncate event_info;
    truncate order_info;
    truncate stats_device_browser;
    truncate stats_device_location;
    truncate stats_event;
    truncate stats_hourly;
    truncate stats_inbound;
    truncate stats_order;
    truncate stats_user;
    truncate stats_view_depth;
 **/
public class NewUserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewUserRunner(), args);
        } catch (Exception e) {
            logger.warn("新增用户运行失败", e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output-mapping.xml");
        conf.addResource("writer-mapping.xml");
//        this.conf = HBaseConfiguration.create(conf); //???
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "new user");
        job.setJarByClass(NewUserRunner.class);

        // 设置map相关属性
        TableMapReduceUtil.initTableMapperJob(
                this.builList(job),
                NewUserMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job,
                true);

        // 设置reducer类
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TextOutPutValue.class);

        // 设置输出的类
        job.setOutputFormatClass(IOutputWriterFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 处理输出的参数
     *
     * @param conf
     * @param args
     */
    private void setArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                if (i + 1 < args.length) {
                    date = args[i + 1];
                }
            }
        }
        // 如果时间为空或者非法, 都将默认运行昨天的数据
        if (StringUtils.isEmpty(date) || !TimeUtil.isValidDate(date)) {
            date = TimeUtil.getYesterday();
        }
        // 将时间存储到config中
//        System.out.println("testing-------" + date);
        conf.set(GlobalConstants.RUNNING_DATE, date);
    }

    /**
     * 获取hbase的扫描对象
     *
     * @param job
     * @return
     */
    private List<Scan> builList(Job job) {
        Configuration conf = job.getConfiguration();
        long startDate = TimeUtil.parseString2long(conf.get(GlobalConstants.RUNNING_DATE));
        long endDate = startDate + GlobalConstants.DAY_OF_MILLSECONDS;
        System.out.println(startDate + "------" + endDate);

        Scan scan = new Scan();
        // TODO 此处如果不加""会导致错误
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));
        // 过滤
        FilterList fl = new FilterList();
        fl.addFilter(new SingleColumnValueFilter(
                Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME),
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.alias)));
//                Bytes.toBytes("e_pv")));

        // 扫描那些字段
        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_UUID,
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME
        };
        // 将扫描的字段添加到scan中
//        fl.addFilter(this.getColumnFilter(columns));

        // 设置hbase的表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        // 将过滤器链添加到scan中
        scan.setFilter(fl);

        return Lists.newArrayList(scan); // google的一个包
    }

    /**
     * 设置扫描的字段
     *
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] bytes = new byte[length][];
        for (int i = 0; i < length; i++) {
            bytes[i] = Bytes.toBytes(columns[i]);
        }

        return new MultipleColumnPrefixFilter(bytes);
    }


}
