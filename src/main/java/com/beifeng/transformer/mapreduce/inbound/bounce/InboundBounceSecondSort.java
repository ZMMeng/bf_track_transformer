package com.beifeng.transformer.mapreduce.inbound.bounce;

import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsInboundBounceDimension;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 自定义的二次排序使用到的类
 * Created by 蒙卓明 on 2017/7/8.
 */
public class InboundBounceSecondSort {

    /**
     * 自定义分组类
     */
    public static class InboundBounceGroupingComparator extends WritableComparator{
        public InboundBounceGroupingComparator() {
            super(StatsInboundBounceDimension.class, true);
        }


        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StatsInboundBounceDimension key1 = (StatsInboundBounceDimension) a;
            StatsInboundBounceDimension key2 = (StatsInboundBounceDimension) b;
            return key1.compareTo(key2);
        }
    }

    /**
     * 自定义分区类
     */
    public static class InboundBoucePartitioner extends Partitioner<StatsInboundBounceDimension, IntWritable>{

        private HashPartitioner<StatsCommonDimension, IntWritable> partitioner = new HashPartitioner
                <StatsCommonDimension, IntWritable>();

        public int getPartition(StatsInboundBounceDimension key, IntWritable value, int numPartitions) {
            return partitioner.getPartition(key.getStatsCommon(), value, numPartitions);
        }
    }
}
