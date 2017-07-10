package com.beifeng.transformer.mapreduce.newmembers;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.mapreduce.newusers.TestNewInstallUserMapReduce;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

import org.apache.log4j.Logger;

/**
 * 测试NewMemberMapReduce
 * Created by Administrator on 2017/7/5.
 */
public class TestNewMembersMapReduce {

    private static final Logger logger = Logger.getLogger(TestNewInstallUserMapReduce.class);

    public static void main(String[] args){
        NewMemberMapReduce runner = new NewMemberMapReduce();
        runner.setupRunner(NewMemberMapReduce.class.getSimpleName(), NewMemberMapReduce.class,
                NewMemberMapReduce.NewMemberMapper.class, NewMemberMapReduce.NewMemberReducer
                        .class, StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension.class,
                MapWritableValue.class, TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算新增会员的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}