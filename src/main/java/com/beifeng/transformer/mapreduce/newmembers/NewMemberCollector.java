package com.beifeng.transformer.mapreduce.newmembers;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.mapreduce.IOutputCollector;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Administrator on 2017/7/5.
 */
public class NewMemberCollector implements IOutputCollector {
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


        //设置预处理对象的参数
        int i = 0;
        switch (mapWritableValue.getKpi()) {
            //统计新增用户
            case NEW_MEMBER:
                //活跃用户数的IntWritable对象
                IntWritable newMemberValue = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getPlatform()));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getDate()));
                pstmt.setInt(++i, newMemberValue.get());
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                pstmt.setInt(++i, newMemberValue.get());
                break;
            //统计浏览器维度的新增用户
            case BROWSER_NEW_MEMBER:
                //活跃用户数的IntWritable对象
                newMemberValue = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getPlatform()));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon()
                        .getDate()));
                pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
                pstmt.setInt(++i, newMemberValue.get());
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                pstmt.setInt(++i, newMemberValue.get());
                break;
            //将新增会员ID插入到member_info表中
            case INSERT_MEMBER_INFO:
                Text memberId = (Text) mapWritableValue.getValue().get(new IntWritable(-1));
                pstmt.setString(++i, memberId.toString());
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMS));
                break;
            default:
                throw new RuntimeException("不支持此种类型的KPI输出操作：" + mapWritableValue.getKpi().name);
        }

        //添加到批处理
        pstmt.addBatch();
    }
}
