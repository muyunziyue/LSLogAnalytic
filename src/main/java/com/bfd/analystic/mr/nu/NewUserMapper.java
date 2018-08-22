package com.bfd.analystic.mr.nu;

import com.bfd.analystic.model.dim.StatsCommonDimension;
import com.bfd.analystic.model.dim.StatsUserDimension;
import com.bfd.analystic.model.dim.base.BrowserDimension;
import com.bfd.analystic.model.dim.base.DateDimension;
import com.bfd.analystic.model.dim.base.KpiDimension;
import com.bfd.analystic.model.dim.base.PlatformDimension;
import com.bfd.analystic.model.dim.value.map.TimeOutputValue;
import com.bfd.common.DateEnum;
import com.bfd.common.EventLogConstants;
import com.bfd.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName NewUserMapper
 * @Description TODO 统计新增用户的个数: launch事件中uuid的去重个数
 * @Author Lidexiu
 * @Date 2018/8/20 14:13
 * @Version 1.0
 **/
public class NewUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewUserMapper.class);

    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 从hbase中读数据
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_UUID)));
        String platformName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));


        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(uuid) ) {
            logger.warn("serverTime and uuid can't be null.serverTime: " + serverTime + " uuid: " + uuid);
            return;
        }

        // 构造输出的value
        long longTime = Long.valueOf(serverTime);
        this.v.setId(uuid);
        this.v.setTime(longTime);

        // 初始化输出的key值
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");

        DateDimension dateDimension = DateDimension.buildDate(longTime, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platformName);

        statsCommonDimension.setDateDimension(dateDimension);
        // 循环平台维度输出 all
        // 为什么要输出all维度?
        // 是为了要统计除了平台以外的维度
        for (PlatformDimension pl: platformDimensions) {
            statsCommonDimension.setPlatformDimension(pl);
            statsCommonDimension.setKpiDimension(newUserKpi);

            this.k.setStatsCommonDimension(statsCommonDimension);
            // 设置默认的浏览器维度
            this.k.setBrowserDimension(defaultBrowserDimension);
            // 输出
            context.write(this.k, this.v);
        }


    }
}
