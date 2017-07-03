package com.beifeng.transformer.mr;

import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 配合自定义output进行SQL输出的类
 * Created by 蒙卓明 on 2017/7/2.
 */
public interface IOutputCollector {

    /**
     * 具体执行统计数据插入的方法
     * @param conf
     * @param key
     * @param value
     * @param pstmt
     * @param converter
     * @throws SQLException
     */
    void collect(Configuration conf, BaseDimension key,
                 BaseStatsValueWritable value, PreparedStatement pstmt,
                 IDimensionConverter converter) throws SQLException;
}
