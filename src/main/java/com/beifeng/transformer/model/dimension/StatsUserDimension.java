package com.beifeng.transformer.model.dimension;

import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.dimension.basic.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 进行用户分析(用户基本信息分析和浏览器分析)定义的组合维度
 * Created by 蒙卓明 on 2017/7/2.
 */
public class StatsUserDimension extends StatsDimension {

    //公用的维度信息
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    //浏览器维度信息
    private BrowserDimension browser = new BrowserDimension();

    public StatsUserDimension() {
        super();
    }

    public StatsUserDimension(StatsCommonDimension statsCommon,
                              BrowserDimension browser) {
        this.statsCommon = statsCommon;
        this.browser = browser;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public BrowserDimension getBrowser() {
        return browser;
    }

    public void setBrowser(BrowserDimension browser) {
        this.browser = browser;
    }

    /**
     * 复制实例
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension) {

        //复制公用的维度信息
        StatsCommonDimension statsCommon = StatsCommonDimension.clone
                (dimension.statsCommon);
        //复制浏览器的维度信息
        BrowserDimension browser = new BrowserDimension(dimension.browser
                .getBrowserName(), dimension.browser.getBrowserVersion());
        return new StatsUserDimension(statsCommon, browser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsUserDimension)) return false;

        StatsUserDimension that = (StatsUserDimension) o;

        if (statsCommon != null ? !statsCommon.equals(that.statsCommon) :
                that.statsCommon != null)
            return false;
        return browser != null ? browser.equals(that.browser) : that.browser
                == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommon != null ? statsCommon.hashCode() : 0;
        result = 31 * result + (browser != null ? browser.hashCode() : 0);
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
        StatsUserDimension other = (StatsUserDimension) o;

        //首先比较statsCommon
        int tmp = statsCommon.compareTo(other.statsCommon);
        //判断statsCommon比较结果
        if(tmp != 0){
            //statsCommon比较结果不为零，直接返回statsCommon比较结果
            return tmp;
        }
        //statsCommon比较结果为零，直接返回browser比较结果
        return browser.compareTo(other.browser);
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        statsCommon.write(out);
        browser.write(out);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        statsCommon.readFields(in);
        browser.readFields(in);
    }
}
