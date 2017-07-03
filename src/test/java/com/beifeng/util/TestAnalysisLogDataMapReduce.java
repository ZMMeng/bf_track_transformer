package com.beifeng.util;

import com.beifeng.etl.mr.ald.AnalysisLogDataMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by 蒙卓明 on 2017/7/1.
 */
public class TestAnalysisLogDataMapReduce {

    private static final Logger logger = Logger.getLogger
            (TestAnalysisLogDataMapReduce.class);

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new AnalysisLogDataMapReduce
                    (), args);
        } catch (Exception e) {
            logger.error("执行日志解析job异常", e);
            throw new RuntimeException(e);
        }

    }
}
