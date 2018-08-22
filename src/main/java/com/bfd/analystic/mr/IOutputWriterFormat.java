package com.bfd.analystic.mr;

import com.bfd.analystic.model.dim.BaseStateDimension;
import com.bfd.analystic.model.dim.value.BaseOutPutValueWritable;
import com.bfd.analystic.model.dim.value.reduce.TextOutPutValue;
import com.bfd.analystic.mr.service.IDimensionConvert;
import com.bfd.analystic.mr.service.impl.IDimensionConvertImpl;
import com.bfd.common.GlobalConstants;
import com.bfd.common.KpiType;
import com.bfd.util.jdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName IOutputWriterFormat
 * @Description TODO 自定义reduce阶段输出格式类
 * @Author Lidexiu
 * @Date 2018/8/21 11:34
 * @Version 1.0
 **/
// IOutputWriterFormat<K extends BaseStateDimension, V extends BaseOutPutValueWritable>
public class IOutputWriterFormat extends OutputFormat<BaseStateDimension, BaseOutPutValueWritable> {
    private static final Logger logger = Logger.getLogger(IOutputWriterFormat.class);
//    DBOutputFormat

    @Override
    public RecordWriter<BaseStateDimension, BaseOutPutValueWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        Connection conn = jdbcUtil.getConn();
        IDimensionConvert convert = new IDimensionConvertImpl();
        return new IOutputRecordWriter(conn, conf, convert);
    }

    // 检测输出空间
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext), taskAttemptContext);
//        return new FileOutputCommitter(null, taskAttemptContext);
    }

    /**
     * 封装输出记录的内部类
     */
    public class IOutputRecordWriter extends RecordWriter<BaseStateDimension, BaseOutPutValueWritable> {

        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvert convert = null;
        // 定义两个集合用于做缓存
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();

        public IOutputRecordWriter() {

        }
        public IOutputRecordWriter(Connection conn, Configuration conf, IDimensionConvert convert) {
            this.conn = conn;   // 这是mysql的连接
            this.conf = conf;   // 这是从任务上下文获取的configuration
            this.convert = convert; // 这是IDimensionConvertImpl对象, 用于根据维度获取维度的id
        }

        @Override
        public void write(BaseStateDimension key, BaseOutPutValueWritable value) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }
//            System.out.println("KpiType: " + value.getKpi());

            try {
                // 获取kpi
                KpiType kpi = value.getKpi();
//                KpiType kpi = KpiType.NEW_USER;
                PreparedStatement ps = null;
                int counter = 1;
                if (map.containsKey(kpi)) {
                    ps = map.get(kpi);
                    counter = this.batch.get(kpi);
                    counter++;
                } else {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi, ps);
                }

                // 将count添加到batch中
                this.batch.put(kpi, counter);

                // 为ps赋值    writer_new_user
                String writerClassName = conf.get(GlobalConstants.WRITER_PREFIX + kpi.kpiName);
                Class<?> classz = Class.forName(writerClassName);
                IOutputWriter writer = (IOutputWriter) classz.newInstance(); // 将类转换成接口
                writer.writer(conf, key, value, ps, convert); // 调用对应的实现类

                // 将赋值好的ps达到一个批量就可以批量处理了
                if (counter % GlobalConstants.BATCH_NUMBER == 0) {
                    ps.executeBatch();
                    conn.commit();
                    this.batch.remove(kpi); // 移除已经执行的kpi的ps
                }

            } catch (Exception e) {
                logger.warn("执行写recordWriter的write方法失败", e);
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            // 循环map并将其中的ps执行
            try {
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    en.getValue().executeBatch(); // 将剩余的ps执行
                }
            } catch (SQLException e) {
                //  再次执行一次
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    try {
                        en.getValue().executeBatch(); // 将剩余的ps执行
                    } catch (SQLException e1) {
                        logger.error("执行close时, 执行剩余的ps错误", e);
                    }
                }
            } finally {
                jdbcUtil.close(conn,null, null);
                // 循环将执行完成后的ps移除
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                        jdbcUtil.close(null,en.getValue(), null);
                }
            }
        }
    }
}
