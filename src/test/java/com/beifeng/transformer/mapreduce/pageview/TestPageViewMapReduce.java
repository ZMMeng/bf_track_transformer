package com.beifeng.transformer.mapreduce.pageview;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2017/7/6.
 */
public class TestPageViewMapReduce {

    private static final Logger logger = Logger.getLogger(TestPageViewMapReduce.class);

    public static void main(String[] args){
        try {
            new ToolRunner().run(new Configuration(), new PageViewMapReduce(), args);
        } catch (Exception e) {
            logger.error("运行计算页面浏览的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
