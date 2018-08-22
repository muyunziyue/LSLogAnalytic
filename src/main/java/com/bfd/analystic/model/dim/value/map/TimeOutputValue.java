package com.bfd.analystic.model.dim.value.map;

import com.bfd.analystic.model.dim.value.BaseOutPutValueWritable;
import com.bfd.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName TimeOutputValue
 * @Description TODO 用于map阶段输出的value的类型
 * @Author Lidexiu
 * @Date 2018/8/20 20:11
 * @Version 1.0
 **/
public class TimeOutputValue extends BaseOutPutValueWritable {

    private String id; // 泛指id
    private long time;

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeLong(time);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
