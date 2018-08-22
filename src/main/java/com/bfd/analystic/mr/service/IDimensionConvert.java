package com.bfd.analystic.mr.service;

import com.bfd.analystic.model.dim.base.BaseDimension;

/**
 * @ClassName IDimensionConvert
 * @Description TODO 根据各个基础维度对象获取在数据库中对应的维度id
 * @Author Lidexiu
 * @Date 2018/8/21 9:32
 * @Version 1.0
 **/
public interface IDimensionConvert {
    /**
     * 根据维度获取id
     * @param dimension
     * @return
     */
    int getDimensionIdByValue(BaseDimension dimension);
}
