package com.beifeng.transformer.mapreduce.inbound.bounce;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsInboundBounceDimension;
import com.beifeng.transformer.model.dimension.StatsInboundDimension;
import com.beifeng.transformer.model.value.reduce.InboundBounceReducerOutputValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

/**
 * Created by 蒙卓明 on 2017/7/8.
 */
public class TestInboundBounceMapReduce {

    private static final Logger logger = Logger.getLogger(TestInboundBounceMapReduce.class);

    public static void main(String[] args) {
        InboundBounceMapReduce runner = new InboundBounceMapReduce();
        runner.setupRunner(InboundBounceMapReduce.class.getSimpleName(), InboundBounceMapReduce.class,
                InboundBounceMapReduce.InboundBounceMapper.class, InboundBounceMapReduce
                        .InboundBounceReducer.class, StatsInboundBounceDimension.class, IntWritable.class,
                StatsInboundDimension.class, InboundBounceReducerOutputValue.class, TransformerOutputFormat
                        .class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算外链跳出会话个数的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
