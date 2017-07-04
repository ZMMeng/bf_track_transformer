package com.beifeng.transformer.mapreduce.totalusers;

import com.beifeng.common.DateEnum;
import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.utils.JdbcManager;
import com.beifeng.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 计算总用户数
 * Created by Administrator on 2017/7/4.
 */
public class TotalInstallUserCalculate {

    //日志打印对象
    private static final Logger logger = Logger.getLogger(TotalInstallUserCalculate.class);
    //数据库连接对象
    Connection conn = null;
    //预处理对象
    PreparedStatement pstmt = null;
    //结果集对象
    ResultSet rs = null;

    /**
     * 计算总用户
     * 当天的总用户数 = 前一天的总用户数 + 当天的新增用户数
     *
     * @param conf
     */
    public void calculateTotalUsers(Configuration conf) {

        try {
            //先处理stats_user表
            //获取当天所对应的时间戳，yyyy年MM月dd日 00:00:00.000
            long date = TimeUtil.parseString2Long(conf.get(GlobalConstants
                    .RUNNING_DATE_PARAMS));
            //获取当天的日期维度信息
            DateDimension todayDimension = DateDimension.buildDate(date, DateEnum
                    .DAY);
            //获取前一天的日期维度信息
            DateDimension yesterdayDimension = DateDimension.buildDate(date -
                    GlobalConstants.MILLISECONDS_OF_DAY, DateEnum.DAY);
            //获取日期id
            conn = JdbcManager.getConnection(conf, GlobalConstants
                    .WAREHOUSE_OF_REPORT);
            //获取前一天的日期id
            int yesterdayDimensionId = getDimensionIdByDate(yesterdayDimension);
            //获取当天的日期id
            int todayDimensionId = getDimensionIdByDate(todayDimension);
            //获取前一天的原始数据，存储格式为dateid_platformid = totalusers
            //该Map集合用于存储到目前为止的总用户数(分平台)
            Map<String, Integer> oldValueMap = new HashMap<String, Integer>();

            //获取前一天的总用户数，并放入oldValueMap中
            if (yesterdayDimensionId != -1) {
                StoreYesterdayTotalInstallUsers2Map(oldValueMap, yesterdayDimensionId);
            }
            //添加当天的新用户，并计算当天的总用户数，并放入到oldValueMap中
            addTodayNewInstallUsers2Map(oldValueMap, todayDimensionId);
            //更新stats_user表
            updateStatsUserTable(oldValueMap, todayDimensionId);

            //再处理stats_device_browser表
            //清空oldMapValue
            oldValueMap.clear();
            //获取前一天的总用户数，并放入oldValueMap中
            if (yesterdayDimensionId != -1) {
                StoreYesterdayBrowserTotalInstallUsers2Map(oldValueMap, yesterdayDimensionId);
            }
            //添加当天的新用户，并计算当天的总用户数，并放入到oldValueMap中
            addTodayBrowserNewInstallUsers2Map(oldValueMap, todayDimensionId);
            //更新stats_device_browser表
            updateStatsDeviceBrowserTable(oldValueMap, todayDimensionId);
        } catch (SQLException e) {
            logger.error("计算总用户数出现异常", e);
        } finally {
            JdbcManager.close(conn, pstmt, rs);
        }

    }

    /**
     * 获取给定日期在dimension_date表中对应的id
     *
     * @param dateDimension 给定日期对应的日期维度对象
     * @return 给定日期在dimension_date表中对应的id，如果没有找到，则返回-1
     * @throws SQLException
     */
    private int getDimensionIdByDate(DateDimension dateDimension) throws
            SQLException {
        //存储给定日期在dimension_date表中对应的id
        int dateDimensionId = -1;
        //进行数据库查询
        pstmt = conn.prepareStatement("select id from dimension_date where year=? and season=? and month=? " +
                "and week=? and day=? and type=? and calendar=?");
        int i = 0;
        pstmt.setInt(++i, dateDimension.getYear());
        pstmt.setInt(++i, dateDimension.getSeason());
        pstmt.setInt(++i, dateDimension.getMonth());
        pstmt.setInt(++i, dateDimension.getWeek());
        pstmt.setInt(++i, dateDimension.getDay());
        pstmt.setString(++i, dateDimension.getType());
        pstmt.setDate(++i, new Date(dateDimension
                .getCalender().getTime()));
        rs = pstmt.executeQuery();
        //判断结果集中是否有记录
        if (rs.next()) {
            dateDimensionId = rs.getInt(1);
        }
        return dateDimensionId;
    }

    /**
     * 从stats_user表中获取给定日期前一日的总用户数(按平台分类)，并按平台存入Map集合中
     *
     * @param map                  存储平台以及其对应的用户数
     * @param yesterdayDimensionId 前一日在dimension_date表中的id
     * @throws SQLException
     */
    private void StoreYesterdayTotalInstallUsers2Map(Map<String, Integer> map, int yesterdayDimensionId)
            throws SQLException {
        pstmt = conn.prepareStatement("select platform_dimension_id,total_install_users from stats_user " +
                "where date_dimension_id=?");
        pstmt.setInt(1, yesterdayDimensionId);
        rs = pstmt.executeQuery();

        while (rs.next()) {
            int platformId = rs.getInt("platform_dimension_id");
            int totalInstallUsers = rs.getInt("total_install_users");
            //将查询出来的平台名称以及相应的总用户数放入到Map集合中
            map.put("" + platformId, totalInstallUsers);
        }
    }

    /**
     * 从stats_device_browser表中获取给定日期前一日的总用户数(按平台和浏览器分类)，并按平台存入Map集合中
     *
     * @param map                  存储平台以及其对应的用户数
     * @param yesterdayDimensionId 前一日在dimension_date表中的id
     * @throws SQLException
     */
    private void StoreYesterdayBrowserTotalInstallUsers2Map(Map<String, Integer> map, int
            yesterdayDimensionId) throws SQLException {
        pstmt = conn.prepareStatement("select platform_dimension_id,browser_dimension_id, " +
                "total_install_users from stats_device_browser where date_dimension_id=?");
        pstmt.setInt(1, yesterdayDimensionId);
        rs = pstmt.executeQuery();

        while (rs.next()) {
            int platformId = rs.getInt("platform_dimension_id");
            int browserId = rs.getInt("browser_dimension_id");
            int totalInstallUsers = rs.getInt("total_install_users");
            //将查询出来的平台名称以及相应的总用户数放入到Map集合中
            map.put(platformId + "_" + browserId, totalInstallUsers);
        }
    }


    /**
     * 获取当天的新增用户(按平台分类)，并与Map集合中存有前一天的总用户数相加(按平台分类)，所得值存入Map集合中
     *
     * @param map              存储平台以及其对应的用户数
     * @param todayDimensionId 当天在dimension_date表中的id
     * @throws SQLException
     */
    private void addTodayNewInstallUsers2Map(Map<String, Integer> map, int todayDimensionId) throws
            SQLException {
        pstmt = conn.prepareStatement("select platform_dimension_id,new_install_users from stats_user " +
                "where date_dimension_id=?");
        pstmt.setInt(1, todayDimensionId);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            int platformId = rs.getInt("platform_dimension_id");
            int newInstallUsers = rs.getInt("new_install_users");
            //查看该平台是否已有用户
            if (map.containsKey(platformId)) {
                //该平台已有用户，将前一天的总用户数加上当天的新增用户数，即为当天的总用户数
                newInstallUsers += map.get("" + platformId);
            }
            //将当天的总用户数存入Map集合
            map.put("" + platformId, newInstallUsers);
        }

    }

    /**
     * 获取当天的新增用户(按平台和浏览器分类)，并与Map集合中存有前一天的总用户数相加(按平台和浏览器分类)，所得值存入Map集合中
     *
     * @param map              存储平台和浏览器以及其对应的用户数
     * @param todayDimensionId 当天在dimension_date表中的id
     * @throws SQLException
     */
    private void addTodayBrowserNewInstallUsers2Map(Map<String, Integer> map, int todayDimensionId) throws
            SQLException {
        pstmt = conn.prepareStatement("select platform_dimension_id,browser_dimension_id,new_install_users" +
                " from stats_device_browser where date_dimension_id=?");
        pstmt.setInt(1, todayDimensionId);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            int platformId = rs.getInt("platform_dimension_id");
            int browserId = rs.getInt("browser_dimension_id");
            int newInstallUsers = rs.getInt("new_install_users");
            String key = platformId + "_" + browserId;
            //查看该平台是否已有用户
            if (map.containsKey(key)) {
                //该平台已有用户，将前一天的总用户数加上当天的新增用户数，即为当天的总用户数
                newInstallUsers += map.get(key);
            }
            //将当天的总用户数存入Map集合
            map.put(key, newInstallUsers);
        }
    }

    /**
     * 更新stats_user表，将当天的总用户数插入stats_user表中
     *
     * @param map
     * @param todayDimensionId
     * @throws SQLException
     */
    private void updateStatsUserTable(Map<String, Integer> map, int todayDimensionId) throws SQLException {
        pstmt = conn.prepareStatement("insert into stats_user (platform_dimension_id,date_dimension_id," +
                "total_install_users) values (?,?,?) on duplicate key update total_install_users=?");

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            pstmt.setInt(1, Integer.valueOf(entry.getKey()));
            pstmt.setInt(2, todayDimensionId);
            pstmt.setInt(3, entry.getValue());
            pstmt.setInt(4, entry.getValue());
            pstmt.execute();
        }
    }

    /**
     * 更新stats_device_browser表，将当天的总用户数插入stats_user表中
     *
     * @param map
     * @param todayDimensionId
     * @throws SQLException
     */
    private void updateStatsDeviceBrowserTable(Map<String, Integer> map, int todayDimensionId)
            throws SQLException {
        pstmt = conn.prepareStatement("insert into stats_device_browser (platform_dimension_id," +
                "browser_dimension_id,date_dimension_id,total_install_users) values (?,?,?,?) on duplicate " +
                "key update total_install_users=?");

        int platformId, browserId;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            //先从Map集合的key(platformId_browserId)中获取platformId和browserId
            String[] strs = entry.getKey().split("_");
            platformId = Integer.valueOf(strs[0]);
            browserId = Integer.valueOf(strs[1]);
            //更新数据库
            pstmt.setInt(1, platformId);
            pstmt.setInt(2, browserId);
            pstmt.setInt(3, todayDimensionId);
            pstmt.setInt(4, entry.getValue());
            pstmt.setInt(5, entry.getValue());
            pstmt.execute();
        }
    }
}
