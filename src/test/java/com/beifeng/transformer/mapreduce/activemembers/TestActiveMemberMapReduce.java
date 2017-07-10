package com.beifeng.transformer.mapreduce.activemembers;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 测试ActiveMemberMapReduce
 * Created by 蒙卓明 on 2017/7/5.
 */
public class TestActiveMemberMapReduce {

    private static final Logger logger = Logger.getLogger(TestActiveMemberMapReduce.class);

    public static void main(String[] args) {
        ActiveMemberMapReduce runner = new ActiveMemberMapReduce();
        runner.setupRunner(ActiveMemberMapReduce.class.getSimpleName(), ActiveMemberMapReduce.class,
                ActiveMemberMapReduce.ActiveMemberMapper.class, ActiveMemberMapReduce.ActiveMemberReducer
                        .class, StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension.class,
                MapWritableValue.class, TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算活跃会员的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
