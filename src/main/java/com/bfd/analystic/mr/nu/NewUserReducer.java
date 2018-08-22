package com.bfd.analystic.mr.nu;

import com.bfd.analystic.model.dim.StatsUserDimension;
import com.bfd.analystic.model.dim.value.reduce.TextOutPutValue;
import com.bfd.analystic.model.dim.value.map.TimeOutputValue;
import com.bfd.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName NewUserReducer
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/20 20:15
 * @Version 1.0
 **/
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TextOutPutValue> {
    private StatsUserDimension k = new StatsUserDimension();
    private TextOutPutValue v = new TextOutPutValue();
    private Set<String> unique = new HashSet<>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        /**
         * 数据类型:
         * 2018-08-20 website list((789, 12345675551238))
         * 2018-08-20 ios list((789, 12345675551238))
         *
         * 2018-08-20 all list((789, 12345675551238),(788, 12345675551238))
         */
        unique.clear();

        for (TimeOutputValue timeOutputValue : values) {
            unique.add(timeOutputValue.getId());
        }

        // 构造输出的value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1), new IntWritable(unique.size()));
        v.setValue(map);
        // 设置kpi
        if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.NEW_USER.kpiName)){
            v.setKpi(KpiType.NEW_USER);
        }
        context.write(key, v);
    }
}
