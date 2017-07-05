package com.beifeng.transformer.mapreduce.sessions;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.mapreduce.IOutputCollector;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Administrator on 2017/7/5.
 */
public class SessionsCollector implements IOutputCollector {
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

        //将key和value强转
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;

        int i = 0;
        IntWritable sessions = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
        IntWritable lengthOfSessions = (IntWritable) mapWritableValue.getValue().get(new
                IntWritable(-2));
        switch (mapWritableValue.getKpi()) {
            case SESSIONS:
                //stats_user表
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getPlatform()));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getDate()));
                pstmt.setInt(++i, sessions.get());
                pstmt.setInt(++i, lengthOfSessions.get());
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                pstmt.setInt(++i, sessions.get());
                pstmt.setInt(++i, lengthOfSessions.get());
                break;
            case BROWSER_SESSIONS:
                //stats_device_browser表
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getPlatform()));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getDate()));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
                pstmt.setInt(++i, sessions.get());
                pstmt.setInt(++i, lengthOfSessions.get());
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                pstmt.setInt(++i, sessions.get());
                pstmt.setInt(++i, lengthOfSessions.get());
                break;
            default:
                throw new RuntimeException("不支持此种类型的KPI输出操作：" + mapWritableValue.getKpi().name);
        }

        //添加到批处理
        pstmt.addBatch();

    }
}
