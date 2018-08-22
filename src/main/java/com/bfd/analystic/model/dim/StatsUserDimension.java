package com.bfd.analystic.model.dim;

import com.bfd.analystic.model.dim.base.BaseDimension;
import com.bfd.analystic.model.dim.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName StatsUserDimension
 * @Description TODO 可用于map和reduce阶段的用户模块和浏览器模块输出的key
 * @Author Lidexiu
 * @Date 2018/8/20 19:30
 * @Version 1.0
 **/
public class StatsUserDimension extends BaseStateDimension {
    private BrowserDimension browserDimension = new BrowserDimension();
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();


    public StatsUserDimension() {

    }

    public StatsUserDimension(BrowserDimension browserDimension, StatsCommonDimension statsCommonDimension) {
        this.browserDimension = browserDimension;
        this.statsCommonDimension = statsCommonDimension;
    }

    /**
     * 克隆StatsUserDimension
     *
     * @param statsUserDimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension statsUserDimension) {
        BrowserDimension browserDimension = new BrowserDimension(
                statsUserDimension.browserDimension.getBrowserName(),
                statsUserDimension.browserDimension.getBrowserVersion()
        );
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(statsUserDimension.statsCommonDimension);

        return new StatsUserDimension(browserDimension, statsCommonDimension);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        this.browserDimension.write(out);
        this.statsCommonDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.browserDimension.readFields(in);
        this.statsCommonDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int map = this.browserDimension.compareTo(other.browserDimension);
        if (map != 0) {
            return map;
        }
        return this.statsCommonDimension.compareTo(other.statsCommonDimension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(browserDimension, that.browserDimension) &&
                Objects.equals(statsCommonDimension, that.statsCommonDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(browserDimension, statsCommonDimension);
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }
}
