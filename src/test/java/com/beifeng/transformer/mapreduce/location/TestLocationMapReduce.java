package com.beifeng.transformer.mapreduce.location;

import com.beifeng.transformer.model.dimension.StatsLocationDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.LocationReducerOutputValue;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Administrator on 2017/7/7.
 */
public class TestLocationMapReduce {

    private static final Logger logger = Logger.getLogger(TestLocationMapReduce.class);

    public static void main(String[] args) {
        LocationMapReduce runner = new LocationMapReduce();
        runner.setupRunner(LocationMapReduce.class.getSimpleName(), LocationMapReduce.class,
                LocationMapReduce.LocationMapper.class, LocationMapReduce.LocationReducer.class,
                StatsLocationDimension.class, TextsOutputValue.class, StatsLocationDimension.class,
                LocationReducerOutputValue.class);
        try {
            runner.run(args);
        } catch (Exception e) {
            logger.error("执行统计地域信息的MapReduce Job出现异常", e);
            throw new RuntimeException(e);
        }
    }
}
