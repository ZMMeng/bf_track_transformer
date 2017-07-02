package com.beifeng.utils;

import com.beifeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * jdbc管理类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class JdbcManager {

    /**
     * * 根据Hadoop配置信息获取RDBMS MySQL数据库的连接
     *
     * @param conf hadoop配置信息
     * @param flag 区分不同数据源的标志位
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(Configuration conf, String flag)
            throws SQLException {
        //获取Hadoop配置信息中关于jdbc四要素的key
        String driverStr = String.format(GlobalConstants.JDBC_DRIVER, flag);
        String urlStr = String.format(GlobalConstants.JDBC_URL, flag);
        String usernameStr = String.format(GlobalConstants.JDBC_USERNAME, flag);
        String passwordStr = String.format(GlobalConstants.JDBC_PASSWORD, flag);

        //获取Hadoop配置信息中关于jdbc四要素的value
        String driver = conf.get(driverStr);
        String url = conf.get(urlStr);
        String username = conf.get(usernameStr);
        String password = conf.get(passwordStr);

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            //nothing
        }
        return DriverManager.getConnection(url, username, password);
    }
}
