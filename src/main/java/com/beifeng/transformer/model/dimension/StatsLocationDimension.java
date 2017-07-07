package com.beifeng.transformer.model.dimension;

import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.dimension.basic.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 统计地域信息的维度类
 * Created by Administrator on 2017/7/7.
 */
public class StatsLocationDimension extends StatsDimension {

    //公用维度信息
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    //地域维度信息
    private LocationDimension location = new LocationDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommon, LocationDimension location) {
        this.statsCommon = statsCommon;
        this.location = location;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public LocationDimension getLocation() {
        return location;
    }

    public void setLocation(LocationDimension location) {
        this.location = location;
    }

    /**
     * 复制一个实例对象
     *
     * @param dimension
     * @return
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension) {
        StatsCommonDimension statsCommon = dimension.getStatsCommon();
        LocationDimension location = dimension.getLocation();
        return new StatsLocationDimension(statsCommon, location);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocationDimension that = (StatsLocationDimension) o;

        if (statsCommon != null ? !statsCommon.equals(that.statsCommon) : that.statsCommon != null)
            return false;
        return location != null ? location.equals(that.location) : that.location == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommon != null ? statsCommon.hashCode() : 0;
        result = 31 * result + (location != null ? location.hashCode() : 0);
        return result;
    }

    /**
     * 实现比较
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
        StatsLocationDimension other = (StatsLocationDimension) o;
        //首先比较statsCommon
        int tmp = statsCommon.compareTo(other.statsCommon);
        //判断statsCommon比较结果
        if(tmp != 0){
            //statsCommon比较结果不为零，直接返回statsCommon比较结果
            return tmp;
        }
        //statsCommon比较结果为零，直接返回location比较结果
        return location.compareTo(other.getLocation());
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        statsCommon.write(out);
        location.write(out);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        statsCommon.readFields(in);
        location.readFields(in);
    }
}