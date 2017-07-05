package com.beifeng.transformer.mapreduce.newmembers;

import com.beifeng.transformer.mapreduce.newusers.TestNewInstallUserMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2017/7/5.
 */
public class TestNewMembersMapReduce {

    private static final Logger logger = Logger.getLogger(TestNewInstallUserMapReduce.class);

    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(), new NewMemberMapReduce(), args);
        } catch (Exception e) {
            logger.error("运行计算新增会员的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}