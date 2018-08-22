package com.bfd.analystic.mr;

import com.bfd.analystic.model.dim.BaseStateDimension;
import com.bfd.analystic.model.dim.value.BaseOutPutValueWritable;
import com.bfd.analystic.mr.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * @ClassName IOutputWriter
 * @Description TODO 为每一个指标的sql语句赋值的接口
 * @Author Lidexiu
 * @Date 2018/8/21 11:28
 * @Version 1.0
 **/
public interface IOutputWriter {
    /**
     * 为每一个指标的sql语句赋值
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param convert
     */
    void writer(Configuration conf, BaseStateDimension key , BaseOutPutValueWritable value,
                 PreparedStatement ps, IDimensionConvert convert);
}
