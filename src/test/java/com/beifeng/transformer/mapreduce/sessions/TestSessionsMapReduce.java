package com.beifeng.transformer.mapreduce.sessions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2017/7/5.
 */
public class TestSessionsMapReduce {

    private static final Logger logger = Logger.getLogger(TestSessionsMapReduce.class);

    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(), new SessionsMapReduce(), args);
        } catch (Exception e) {
            logger.error("运行计算会话个数和会话长度的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
