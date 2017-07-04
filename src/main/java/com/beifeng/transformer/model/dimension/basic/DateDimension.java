package com.beifeng.transformer.model.dimension.basic;

import com.beifeng.common.DateEnum;
import com.beifeng.utils.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期维度信息类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class DateDimension extends BaseDimension {

    //id
    private int id;
    //年份
    private int year;
    //季度
    private int season;
    //月份
    private int month;
    //周，特指一年中第几周
    private int week;
    //天
    private int day;
    //类型
    private String type;
    //当前日期
    private Date calender = new Date();

    public DateDimension() {
        super();
    }

    public DateDimension(int year, int season, int month, int week, int day,
                         String type) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
    }

    public DateDimension(int year, int season, int month, int week, int day,
                         String type, Date calender) {
        this(year, season, month, week, day, type);
        this.calender = calender;
    }

    public DateDimension(int id, int year, int season, int month, int week,
                         int day, String type, Date calender) {
        this(year, season, month, week, day, type, calender);
        this.id = id;
    }

    /**
     * 根据给定的type类型获取对应的日期维度对象
     *
     * @param time 时间戳
     * @param type 类型
     * @return
     */
    public static DateDimension buildDate(long time, DateEnum type) {

        //创建并清空日期对象
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        //从时间戳中获取年份信息
        int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);
        if (DateEnum.YEAR.equals(type)) {
            //获取只有年份信息的DateDimension对象
            //日期对象设置为该年的第一天
            calendar.set(year, 0, 1);
            return new DateDimension(year, 0, 0, 0, 0, type.name,
                    calendar.getTime());
        }
        //从时间戳中获取季度信息
        int season = TimeUtil.getDateInfo(time, DateEnum.SEASON);
        if (DateEnum.SEASON.equals(type)) {
            //获取只有年份信息和季度信息的DateDimension对象
            //首先获取该季度的第一个月
            int month = 3 * season - 2;
            //日期对象设置为该年该季度的第一天
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, 0, 0, 0, type.name,
                    calendar.getTime());
        }
        //从时间戳中获取月份信息
        int month = TimeUtil.getDateInfo(time, DateEnum.MONTH);
        if (DateEnum.MONTH.equals(type)) {
            //获取只有年份信息、季度信息和月份信息的DateDimension对象
            //日期对象设置为该年该月的第一天
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 0, type.name,
                    calendar.getTime());
        }
        //从时间戳中获取周信息
        int week = TimeUtil.getDateInfo(time, DateEnum.WEEK);
        if (DateEnum.WEEK.equals(type)) {
            //获取只有年份信息、季度信息、月份信息和周信息的DateDimension对象
            //获取给定时间戳所属周的第一天所对应的时间戳
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            //考虑周可能跨年、跨季度和跨月的情况，所以year、season和month都需要更新
            year = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.MONTH);
            week = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.WEEK);

            //判断这一周是否是跨年周
            if (month == 12 && week == 1) {
                //如果一个星期是跨月年份的话，无论是哪个年份，Calendar.WEEK_OF_YEAR都是1
                //显然要将它修改为第53周
                week = 53;
            }

            return new DateDimension(year, season, month, week, 0, type.name,
                    new Date(firstDayOfWeek));
        }
        //从时间戳中获取天信息
        int day = TimeUtil.getDateInfo(time, DateEnum.DAY);
        if (DateEnum.DAY.equals(type)) {
            //获取只有年份信息、季度信息和月份信息的DateDimension对象
            //日期对象设置为该年该月该天
            calendar.set(year, month - 1, day);
            //判断这一周是否是跨年周
            if (month == 12 && week == 1) {
                //如果一个星期是跨月年份的话，无论是哪个年份，Calendar.WEEK_OF_YEAR都是1
                //显然要将它修改为第53周
                week = 53;
            }
            return new DateDimension(year, season, month, week, day, type.name,
                    calendar.getTime());
        }

        throw new RuntimeException("不支持所要求的DateEnum类型来获取DateDimension对象：" +
                type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCalender() {
        return calender;
    }

    public void setCalender(Date calender) {
        this.calender = calender;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DateDimension)) return false;

        DateDimension that = (DateDimension) o;

        if (id != that.id) return false;
        if (year != that.year) return false;
        if (season != that.season) return false;
        if (month != that.month) return false;
        if (week != that.week) return false;
        if (day != that.day) return false;
        return type != null ? type.equals(that.type) : that.type == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + year;
        result = 31 * result + season;
        result = 31 * result + month;
        result = 31 * result + week;
        result = 31 * result + day;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    /**
     * 实现比较
     *
     * @param o
     * @return
     */
    public int compareTo(BaseDimension o) {
        //判断是否是同一个对象
        if (this == o) {
            //是同一对象则直接返回相等
            return 0;
        }
        //不是同一对象则将o强转
        DateDimension other = (DateDimension) o;

        //首先比较id
        int tmp = Integer.valueOf(id).compareTo(other.id);
        //判断id比较结果是否为零
        if (tmp != 0) {
            //id比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //id比较结果为零，比较year
        tmp = Integer.valueOf(year).compareTo(other.year);
        //判断year比较结果是否为零
        if (tmp != 0) {
            //year比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //year比较结果为零，比较season
        tmp = Integer.valueOf(season).compareTo(other.season);
        //判断season比较结果是否为零
        if (tmp != 0) {
            //season比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //season比较结果为零，比较month
        tmp = Integer.valueOf(month).compareTo(other.month);
        //判断month比较结果是否为零
        if (tmp != 0) {
            //month比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //month比较结果为零，比较week
        tmp = Integer.valueOf(week).compareTo(other.week);
        //判断week比较结果是否为零
        if (tmp != 0) {
            //week比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //week比较结果为零，比较day
        tmp = Integer.valueOf(day).compareTo(other.day);
        //判断day比较结果是否为零
        if (tmp != 0) {
            //day比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //day比较结果为零，直接返回type的比较结果
        return String.valueOf(type).compareTo(other.type);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(year);
        out.writeInt(season);
        out.writeInt(month);
        out.writeInt(week);
        out.writeInt(day);
        out.writeUTF(type);
        out.writeLong(calender.getTime());
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        year = in.readInt();
        season = in.readInt();
        month = in.readInt();
        week = in.readInt();
        day = in.readInt();
        type = in.readUTF();
        calender.setTime(in.readLong());
    }
}