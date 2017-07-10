package com.beifeng.transformer.mapreduce.pageview;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 * 测试PageViewMapReduce
 * Created by Administrator on 2017/7/6.
 */
public class TestPageViewMapReduce {

    private static final Logger logger = Logger.getLogger(TestPageViewMapReduce.class);

    public static void main(String[] args) {
        PageViewMapReduce runner = new PageViewMapReduce();
        runner.setupRunner(PageViewMapReduce.class.getSimpleName(), PageViewMapReduce.class,
                PageViewMapReduce.PageViewMapper.class, PageViewMapReduce.PageViewReducer.class,
                StatsUserDimension.class, NullWritable.class, StatsUserDimension.class, MapWritableValue
                        .class, TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算页面浏览的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
