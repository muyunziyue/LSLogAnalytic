package com.bfd.etl.mr.tohbase;

import com.bfd.common.EventLogConstants;
import com.bfd.etl.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * @ClassName ToHbaseMapper
 * @Description 将hdfs中收集的数据清洗后存储得到hbase中
 * @Author Lidexiu
 * @Date 2018/8/17 11:44
 * @Version 1.0
 **/
public class ToHbaseMapper extends Mapper<Object, Text, NullWritable, Put> {
    private static final Logger logger = Logger.getLogger(ToHbaseMapper.class);
    // 输入输出的记录数
    private int inputRecords, outputRecords, filterRecords = 0;
    private final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME);

    private CRC32 crc = new CRC32();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;
        String log = value.toString();
        if (StringUtils.isEmpty(log.trim())) {
            this.filterRecords++;
            return;
        }
        // 正常调动日志工具方法进行解析
        Map<String, String> info = LogUtil.handleLog(log);
        // 根据事件来存储数据
        String eventName = info.getOrDefault(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME,"unknow");
        //TODO 此处逻辑是否有错误,如果获取到的事件类型不在预设的六种之类就会返回null, 导致后面的switch(null)报一个空指针异常
        EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(eventName);
        switch (event) {
            case LAUNCH:
            case PAGEVIEW:
            case CHARGERQUEST:
            case CHARGESUCCESS:
            case CHARGEREFUND:
            case EVENT:
                handleLogToHbase(info, eventName, context);
                break;
            default:
                logger.warn("事件类型暂时不支持数据的清洗.eventName" + eventName);
                this.filterRecords++;
                break;
        }
    }

    /**
     * 将每一行的数据写出
     *
     * @param info
     * @param context
     */
    private void handleLogToHbase(Map<String, String> info, String eventName, Context context) {
        if (!info.isEmpty()) {
            // 获取构造row-key的字段
            String serverTime = info.get(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME);
            String uuid = info.get(EventLogConstants.EVENT_COLUMN_NAME_UUID);
            String umid = info.get(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID);

            try {
                if (StringUtils.isNotEmpty(serverTime)) {
                    // 构建row_key
                    String rowKey = buildRowKey(serverTime, uuid, umid, eventName);
                    //获取hbasePut的对象
                    Put put = new Put(Bytes.toBytes(rowKey));
                    // 循环info, 将所有的k-v数据存储到row-key行中
                    for (Map.Entry<String, String> entry : info.entrySet()) {
                        if (StringUtils.isNotEmpty(entry.getKey())) {
                            // 将kv添加到put中
                            put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                            context.write(NullWritable.get(), put);
                            outputRecords++;

                        } else {
                            this.filterRecords++;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 构建row_key
     *
     * @param serverTime
     * @param uuid
     * @param umid
     * @param eventName
     * @return
     */
    private String buildRowKey(String serverTime, String uuid, String umid, String eventName) {
        StringBuffer sb = new StringBuffer(serverTime + "_");
        if (StringUtils.isNotEmpty(serverTime)) {
            // 对crc32的值进行初始化
            this.crc.reset();
            if (StringUtils.isNotEmpty(uuid)) {
                this.crc.update(uuid.getBytes());
            }
            if (StringUtils.isNotEmpty(umid)) {
                this.crc.update(uuid.getBytes());
            }
            if (StringUtils.isNotEmpty(eventName)) {
                this.crc.update(uuid.getBytes());
            }

            sb.append(this.crc.getValue() % 1000000000L);
        }
        return sb.toString();
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("++++++++++inputRecords: " + inputRecords + " filterRecords: " + filterRecords + " outputRecords: " + outputRecords);
    }
}
