package com.beifeng.transformer.mapreduce.newusers;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

import org.apache.log4j.Logger;

/**
 * 测试NewInstallUserMapReduce
 * Created by Administrator on 2017/7/3.
 */
public class TestNewInstallUserMapReduce {
    private static final Logger logger = Logger.getLogger(TestNewInstallUserMapReduce.class);

    public static void main(String[] args){
        NewInstallUserMapReduce runner = new NewInstallUserMapReduce();
        runner.setupRunner(NewInstallUserMapReduce.class.getSimpleName(), NewInstallUserMapReduce.class,
                NewInstallUserMapReduce.NewInstallUserMapper.class, NewInstallUserMapReduce
                        .NewInstallUserReducer.class, StatsUserDimension.class, TimeOutputValue.class,
                StatsUserDimension.class, MapWritableValue.class, TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算新用户的job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
