package com.beifeng.transformer.model.dimension;

import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.dimension.basic.InboundDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 统计外链相关信息时候的维度类
 * Created by 蒙卓明 on 2017/7/7.
 */
public class StatsInboundDimension extends StatsDimension {

    //公用维度
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    //外链维度
    private InboundDimension inbound = new InboundDimension();

    public StatsInboundDimension() {
    }

    public StatsInboundDimension(StatsCommonDimension statsCommon, InboundDimension inbound) {
        this.statsCommon = statsCommon;
        this.inbound = inbound;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public InboundDimension getInbound() {
        return inbound;
    }

    public void setInbound(InboundDimension inbound) {
        this.inbound = inbound;
    }

    /**
     * 复制实例
     *
     * @param inbound
     * @return
     */
    public static StatsInboundDimension clone(StatsInboundDimension inbound) {
        return new StatsInboundDimension(new StatsCommonDimension(), new InboundDimension());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsInboundDimension)) return false;

        StatsInboundDimension that = (StatsInboundDimension) o;

        if (statsCommon != null ? !statsCommon.equals(that.statsCommon) : that.statsCommon != null)
            return false;
        return inbound != null ? inbound.equals(that.inbound) : that.inbound == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommon != null ? statsCommon.hashCode() : 0;
        result = 31 * result + (inbound != null ? inbound.hashCode() : 0);
        return result;
    }

    /**
     * 实现比较
     *
     * @param o
     * @return
     */
    public int compareTo(BaseDimension o) {
        //判断是否是同一对象
        if (this == o) {
            //同一对象，直接返回0
            return 0;
        }
        //强转
        StatsInboundDimension other = (StatsInboundDimension) o;
        //首先比较statsCommon
        int tmp = statsCommon.compareTo(other.statsCommon);
        //判断statsCommon比较结果
        if (tmp != 0) {
            //statsCommon比较结果不为零，直接返回statsCommon比较结果
            return tmp;
        }
        //statsCommon比较结果为零，直接返回inbound比较结果
        return inbound.compareTo(other.inbound);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        statsCommon.write(out);
        inbound.write(out);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        statsCommon.readFields(in);
        inbound.readFields(in);
    }
}
