package com.beifeng.transformer.service;

import com.beifeng.transformer.model.dim.base.BaseDimension;

import java.io.IOException;

/**
 * 提供专门操作Demension表的接口
 * Created by 蒙卓明 on 2017/7/2.
 */
public interface IDimensionConverter {

    /**
     * 根据dimension的value值获取id
     *
     * @param dimension
     * @return 如果数据库中有直接返回，如果没有，则在数据库中插入后返回新的id
     * @throws IOException
     */
    int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
