package com.beifeng.common;

/**
 * 全局常量类
 * Created by 蒙卓明 on 2017/7/1.
 */
public class GlobalConstants {

    //运行时间变量名
    public static final String RUNNING_DATE_PARAMS = "RUNNING_DATE";
    //默认值
    public static final String DEFAULT_VALUE = "unknown";
    //维度信息表中指定全部列值
    public static final String VALUE_OF_ALL = "all";
    //定义的output collector的前缀
    public static final String OUTPUT_COLLECTOR_KEY_PREFIX = "collector_";
    //Driver名称
    public static final String JDBC_DRIVER = "mysql.%s.driver";
    //jdbc url
    public static final String JDBC_URL = "mysql.%s.url";
    //MySQL用户名
    public static final String JDBC_USERNAME = "mysql.%s.username";
    //MySQL密码
    public static final String JDBC_PASSWORD = "mysql.%s.password";
    //批量执行的key
    public static final String JDBC_BATCH_NUMBER = "mysql.batch.num";
    //默认的批量大小
    public static final String DEFAULT_JDBC_BATCH_NUMBER = "500";
    //指定连接表配置为report
    public static final String WAREHOUSE_OF_REPORT = "report";



}
