package com.beifeng.transformer.model.dim;

import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 公用的维度信息组合
 * Created by 蒙卓明 on 2017/7/2.
 */
public class StatsCommonDimension extends StatsDimension {

    //日期维度
    private DateDimension date = new DateDimension();
    //平台维度
    private PlatformDimension platform = new PlatformDimension();
    //KPI维度
    private KpiDimension kpi = new KpiDimension();

    public StatsCommonDimension() {
        super();
    }

    public StatsCommonDimension(DateDimension date, PlatformDimension
            platform, KpiDimension kpi) {
        this.date = date;
        this.platform = platform;
        this.kpi = kpi;
    }

    public DateDimension getDate() {
        return date;
    }

    public void setDate(DateDimension date) {
        this.date = date;
    }

    public PlatformDimension getPlatform() {
        return platform;
    }

    public void setPlatform(PlatformDimension platform) {
        this.platform = platform;
    }

    public KpiDimension getKpi() {
        return kpi;
    }

    public void setKpi(KpiDimension kpi) {
        this.kpi = kpi;
    }

    /**
     * 复制一个实例对象
     *
     * @param dimension
     * @return
     */
    public static StatsCommonDimension clone(StatsCommonDimension dimension) {
        //复制日期维度信息
        DateDimension date = new DateDimension(dimension.date.getId(),
                dimension.date.getYear(), dimension.date.getSeason(),
                dimension.date.getMonth(), dimension.date.getWeek(),
                dimension.date.getDay(), dimension.date.getType(), dimension
                .date.getCalender());
        //复制平台维度信息
        PlatformDimension platform = new PlatformDimension(dimension.platform
                .getId(), dimension.platform.getPlatformName());
        //复制KPI维度信息
        KpiDimension kpi = new KpiDimension(dimension.kpi.getId(), dimension
                .kpi.getKpiName());
        return new StatsCommonDimension(date, platform, kpi);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsCommonDimension)) return false;

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (date != null ? !date.equals(that.date) : that.date != null)
            return false;
        if (platform != null ? !platform.equals(that.platform) : that
                .platform != null)
            return false;
        return kpi != null ? kpi.equals(that.kpi) : that.kpi == null;
    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + (platform != null ? platform.hashCode() : 0);
        result = 31 * result + (kpi != null ? kpi.hashCode() : 0);
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
        StatsCommonDimension other = (StatsCommonDimension) o;

        //首先比较date
        int tmp = date.compareTo(other.date);
        //判断date比较结果
        if(tmp != 0){
            //date比较结果不为零，直接返回date比较结果
            return tmp;
        }
        //date比较结果为零，接着比较platform
        tmp = platform.compareTo(other.platform);
        //判断platform比较结果
        if(tmp != 0){
            //platform比较结果不为零，直接返回platform比较结果
            return tmp;
        }
        //platform比较结果为零，直接返回kpi的比较结果
        return kpi.compareTo(other.kpi);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        date.write(out);
        platform.write(out);
        kpi.write(out);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        date.readFields(in);
        platform.readFields(in);
        kpi.readFields(in);
    }
}
