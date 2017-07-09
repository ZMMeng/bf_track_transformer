package com.beifeng.transformer.mapreduce.activeusers;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.mapreduce.newusers.TestNewInstallUserMapReduce;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2017/7/4.
 */
public class TestActiveUserMapReduce {

    private static final Logger logger = Logger.getLogger(TestNewInstallUserMapReduce.class);

    public static void main(String[] args) {
        ActiveUserMapReduce runner = new ActiveUserMapReduce();
        runner.setupRunner(ActiveUserMapReduce.class.getSimpleName(), ActiveUserMapReduce.class,
                ActiveUserMapReduce.ActiveUserMapper.class, ActiveUserMapReduce.ActiveUserReducer
                        .class, StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension
                        .class, MapWritableValue.class, TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算活跃用户的job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
