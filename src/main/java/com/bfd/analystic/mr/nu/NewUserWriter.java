package com.bfd.analystic.mr.nu;

import com.bfd.analystic.model.dim.BaseStateDimension;
import com.bfd.analystic.model.dim.StatsUserDimension;
import com.bfd.analystic.model.dim.value.BaseOutPutValueWritable;
import com.bfd.analystic.model.dim.value.reduce.TextOutPutValue;
import com.bfd.analystic.mr.IOutputWriter;
import com.bfd.analystic.mr.service.IDimensionConvert;
import com.bfd.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName NewUserWriter
 * @Description TODO 新增用户的ps执行
 * @Author Lidexiu
 * @Date 2018/8/21 14:20
 * @Version 1.0
 **/
public class NewUserWriter implements IOutputWriter {
    @Override
    public void writer(Configuration conf, BaseStateDimension key, BaseOutPutValueWritable value, PreparedStatement ps, IDimensionConvert convert) {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutPutValue textOutPutValue = (TextOutPutValue) value;
        int newUsers = ((IntWritable)textOutPutValue.getValue().get(new IntWritable(-1))).get();

        int i = 0;
        // 为ps赋值
        // 或者可以抛异常
        try {
            ps.setInt(++i, convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i, newUsers);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, newUsers);

            // 添加到批处理
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
