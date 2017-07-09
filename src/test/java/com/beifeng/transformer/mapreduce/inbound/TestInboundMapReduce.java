package com.beifeng.transformer.mapreduce.inbound;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsInboundDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.InboundReducerOutputValue;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by 蒙卓明 on 2017/7/8.
 */
public class TestInboundMapReduce {

    private static final Logger logger = Logger.getLogger(TestInboundMapReduce.class);

    public static void main(String[] args){
        InboundMapReduce runner = new InboundMapReduce();
        runner.setupRunner(InboundMapReduce.class.getSimpleName(), InboundMapReduce.class, InboundMapReduce
                .InboundMapper.class, InboundMapReduce.InboundReducer.class, StatsInboundDimension.class,
                TextsOutputValue.class, StatsInboundDimension.class, InboundReducerOutputValue.class,
                TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行统计外链信息的会话个数和活跃用户数出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
