package com.beifeng.utils;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 时间控制工具类
 * Created by 蒙卓明 on 2017/7/1.
 */
public class TimeUtil {

    //默认的日期格式
    private static final String DATA_FORMAT = "yyyy-MM-dd";

    /**
     * 获取前一天的日期数据，使用默认的日期格式
     * @return
     */
    public static String getYesterday(){
        return getYesterday(DATA_FORMAT);
    }

    /**
     * 获取前一天的日期数据，使用指定的日期格式
     * @param pattern 指定的日期格式
     * @return
     */
    public static String getYesterday(String pattern){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 判断输入的字符串是否是一个时间有效的时间格式数据
     * @param input
     * @return
     */
    public static boolean isValidateRunningDate(String input){
        if(input == null || input.trim().equals("")){
            return false;
        }
        Matcher matcher = null;
        boolean result = false;
        String regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$";
        Pattern pattern = Pattern.compile(regex);
        matcher = pattern.matcher(input);
        if(matcher != null){
            result = matcher.matches();
        }
        return result;
    }

    /**
     * 将yyyy-MM-dd格式的日期字符串转换为时间戳
     * @param input 日期字符串
     * @return 日期字符串对应的时间戳
     */
    public static long parseString2Long(String input){
        return parseString2Long(input, DATA_FORMAT);
    }

    /**
     * 将指定格式的日期字符串转换为时间戳
     * @param input 日期字符串
     * @param pattern 给定的日期格式
     * @return 日期字符串对应的时间戳
     */
    public static long parseString2Long(String input, String pattern){
        Date date;
        try{
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }

    /**
     * 将时间戳转换为yyyy-MM-dd格式的日期字符串
     * @param input 时间戳
     * @return yyyy-MM-dd格式的日期字符串
     */
    public static String parseLong2String(long input){
        return parseLong2String(input, DATA_FORMAT);
    }

    /**
     * 将时间戳转换为指定格式的日期字符串
     * @param input 时间戳
     * @param pattern 日期格式
     * @return 指定格式的日期字符串
     */
    public static String parseLong2String(long input, String pattern){
        Date date = new Date(input);
        return new SimpleDateFormat(pattern).format(date);
    }

    /**
     * 将nginx服务器时间转换为时间戳
     * @param input
     * @return 解析成功返回时间戳，否则返回-1
     */
    public static long parseNginxServerTime2Long(String input){
        Date date = parseNginxServerTime2Date(input);
        return date == null ? -1 : date.getTime();
    }

    /**
     * 将nginx服务器时间转换为Date对象
     * @param input 格式为1449410796.976
     * @return 解析成功返回相应的Date对象，否则返回null
     */
    public static Date parseNginxServerTime2Date(String input){
        //判断输入的字符串是否为空
        if(StringUtils.isBlank(input)){
            return null;
        }

        try{
            long timestamp = Double.valueOf(Double.valueOf(input.trim()) *
                    1000).longValue();
            return new Date(timestamp);
        }catch (Exception e){
            //nothing
            return null;
        }
    }
}
