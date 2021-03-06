package com.beifeng.transformer.mapreduce.activemembers;

import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.IOutputCollector;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 蒙卓明 on 2017/7/4.
 */
public class ActiveMemberCollector implements IOutputCollector {

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
        //活跃用户数的IntWritable对象
        IntWritable activeMemberValue = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        //设置预处理对象的参数
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        //判断KPI是否需要考虑浏览器维度
        if (KpiType.BROWSER_ACTIVE_MEMBER.name.equals(statsUserDimension.getStatsCommon().getKpi()
                .getKpiName())) {
            pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        }
        pstmt.setInt(++i, activeMemberValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
        pstmt.setInt(++i, activeMemberValue.get());

        //添加到批处理
        pstmt.addBatch();
    }
}