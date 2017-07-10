package com.beifeng.transformer.mapreduce.inbound.bounce;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.mapreduce.IOutputCollector;
import com.beifeng.transformer.model.dimension.StatsInboundDimension;
import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.InboundBounceReducerOutputValue;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 蒙卓明 on 2017/7/8.
 */
public class InboundBounceCollector implements IOutputCollector {
    /**
     * 在Reduce任务完成后，将所得结果插入到相应的表中
     *
     * @param conf      MapReduce Job的配置信息
     * @param key       Reduce输出的key
     * @param value     Reduce输出的value
     * @param pstmt     预处理对象
     * @param converter 操作维度信息的对象
     * @throws SQLException
     * @throws IOException
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
                        PreparedStatement pstmt, IDimensionConverter converter) throws SQLException,
            IOException {
        //强转
        StatsInboundDimension statsInboundDimension = (StatsInboundDimension) key;
        InboundBounceReducerOutputValue inboundBounceReducerOutputValue = (InboundBounceReducerOutputValue)
                value;

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsInboundDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsInboundDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, inboundBounceReducerOutputValue.getBounceSessions());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
        pstmt.setInt(++i, inboundBounceReducerOutputValue.getBounceSessions());

        //添加到批处理
        pstmt.addBatch();
    }
}
