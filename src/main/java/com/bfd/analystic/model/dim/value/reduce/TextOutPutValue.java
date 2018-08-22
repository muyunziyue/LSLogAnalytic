package com.bfd.analystic.model.dim.value.reduce;

import com.bfd.analystic.model.dim.value.BaseOutPutValueWritable;
import com.bfd.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * @ClassName TextOutPutValue
 * @Description TODO 用户模块和浏览器模块reduce阶段输出的value的类型
 * @Author Lidexiu
 * @Date 2018/8/20 20:18
 * @Version 1.0
 **/
public class TextOutPutValue extends BaseOutPutValueWritable {
    private MapWritable value = new MapWritable();
    private KpiType kpi;

    public TextOutPutValue(){

    }

    public TextOutPutValue(KpiType kpi, MapWritable value) {
        this.kpi = kpi;
        this.value = value;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out);
        WritableUtils.writeEnum(out, kpi);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        WritableUtils.readEnum(in, KpiType.class);
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
}
