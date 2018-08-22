package com.bfd.analystic.model.dim.base;

import org.apache.hadoop.io.WritableComparable;

/**
 * @ClassName BaseDimension
 * @Description TODO 维度类的顶级父类, 他的所有子类有所有维度类: 平台, 时间, 浏览器等.
 * @Author Lidexiu
 * @Date 2018/8/20 10:20
 * @Version 1.0
 **/
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
    // do nothing
}
