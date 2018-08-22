package com.bfd.analystic.model.dim.value;

import com.bfd.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @ClassName BaseOutPutValueWritable
 * @Description TODO map和reduce阶段输出的value类型的顶级父类
 *  由于所有输出的value的都不在需要比较, 所以这里不再需要继承WritableComparable接口
 * @Author Lidexiu
 * @Date 2018/8/20 19:59
 * @Version 1.0
 **/
public abstract class BaseOutPutValueWritable implements Writable {
    public abstract KpiType getKpi();
}
