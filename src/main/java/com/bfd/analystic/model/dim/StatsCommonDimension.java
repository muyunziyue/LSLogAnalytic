package com.bfd.analystic.model.dim;

import com.bfd.analystic.model.dim.base.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName StatsCommonDimension
 * @Description TODO map阶段和reduce阶段输出的key的公共维度类型封装
 * @Author Lidexiu
 * @Date 2018/8/20 11:59
 * @Version 1.0
 **/
public class StatsCommonDimension extends BaseStateDimension {
    private DateDimension dateDimension = new DateDimension();
    private PlatformDimension platformDimension = new PlatformDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension(){

    }

    public StatsCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.dateDimension.write(out);
        this.platformDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateDimension.readFields(in);
        this.platformDimension.readFields(in);
        this.kpiDimension.readFields(in);
    }

    public static StatsCommonDimension clone(StatsCommonDimension dimension) {
        PlatformDimension platformDimension = new PlatformDimension(
                dimension.platformDimension.getPlatformNmae());
        DateDimension dateDimension = new DateDimension(
                dimension.dateDimension.getYear(),
                dimension.dateDimension.getSeason(),
                dimension.dateDimension.getMonth(),
                dimension.dateDimension.getWeek(),
                dimension.dateDimension.getDay(),
                dimension.dateDimension.getDay(),
                dimension.dateDimension.getType(),
                dimension.dateDimension.getCalendar()
        );
        KpiDimension kpiDimension = new KpiDimension(dimension.kpiDimension.getKpiName());
        return new StatsCommonDimension(dateDimension, platformDimension, kpiDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.platformDimension.compareTo(other.platformDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.kpiDimension.compareTo(other.kpiDimension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsCommonDimension that = (StatsCommonDimension) o;
        return Objects.equals(dateDimension, that.dateDimension) &&
                Objects.equals(platformDimension, that.platformDimension) &&
                Objects.equals(kpiDimension, that.kpiDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dateDimension, platformDimension, kpiDimension);
    }

    public DateDimension getDateDimension() {

        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {

        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {

        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension()
    {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {

        this.kpiDimension = kpiDimension;
    }
}
