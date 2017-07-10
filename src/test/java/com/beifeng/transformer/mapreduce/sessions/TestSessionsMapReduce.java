package com.beifeng.transformer.mapreduce.sessions;

import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

import org.apache.log4j.Logger;

/**
 * 测试SessionsMapReduce
 * Created by Administrator on 2017/7/5.
 */
public class TestSessionsMapReduce {

    private static final Logger logger = Logger.getLogger(TestSessionsMapReduce.class);

    public static void main(String[] args){
        SessionsMapReduce runner = new SessionsMapReduce();
        runner.setupRunner(SessionsMapReduce.class.getSimpleName(), SessionsMapReduce.class,
                SessionsMapReduce.SessionsMapper.class, SessionsMapReduce.SessionsReducer.class,
                StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension.class, MapWritableValue
                        .class, TransformerOutputFormat.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行计算会话个数和会话长度的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
