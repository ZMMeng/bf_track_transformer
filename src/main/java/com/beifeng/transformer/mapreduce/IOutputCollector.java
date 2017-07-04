package com.beifeng.transformer.mapreduce;

import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 配合自定义output进行SQL输出的类
 * Created by 蒙卓明 on 2017/7/2.
 */
public interface IOutputCollector {

    /**
     * 在Reduce任务完成后，将所得结果插入到相应的表中
     *
     * @param conf MapReduce Job的配置信息
     * @param key Reduce输出的key
     * @param value Reduce输出的value
     * @param pstmt 预处理对象
     * @param converter 操作维度信息的对象
     * @throws SQLException
     * @throws IOException
     */
    void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement
            pstmt, IDimensionConverter converter) throws SQLException, IOException;
}
