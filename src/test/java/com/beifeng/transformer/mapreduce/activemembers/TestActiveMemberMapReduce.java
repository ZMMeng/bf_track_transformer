package com.beifeng.transformer.mapreduce.activemembers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by 蒙卓明 on 2017/7/5.
 */
public class TestActiveMemberMapReduce {

    private static final Logger logger = Logger.getLogger(TestActiveMemberMapReduce.class);

    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(), new ActiveMemberMapReduce(), args);
        } catch (Exception e) {
            logger.error("运行计算活跃会员的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
