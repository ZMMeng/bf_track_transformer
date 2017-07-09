package com.beifeng.transformer.service.impl;

import com.beifeng.common.GlobalConstants;
import com.beifeng.utils.JdbcManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 操作dimension_inbound表的服务提供类
 * Created by 蒙卓明 on 2017/7/7.
 */
public class InboundDimensionService {

    private static final Logger logger = Logger.getLogger(InboundDimensionService.class);
    //全部"all"的id
    public static final int ALL_OF_INBOUND_ID = 1;
    //其他外链的id
    public static final int OTHER_OF_INBOUND_ID = 2;

    /**
     * 获取dimension_inbound表中，url与id的映射关系
     *
     * @param conf
     * @param type
     * @return
     */
    public static Map<String, Integer> getInboundByType(Configuration conf, int type) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            //获取数据库连接
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            pstmt = conn.prepareStatement("select id,url from dimension_inbound where type=?;");
            pstmt.setInt(1, type);
            rs = pstmt.executeQuery();
            Map<String, Integer> result = new HashMap<String, Integer>();
            while (rs.next()) {
                int id = rs.getInt("id");
                String url = rs.getString("url");
                result.put(url, id);
            }
            return result;
        } catch (SQLException e) {
            logger.error("查询dimension_inbound表出现数据库异常", e);
            throw new RuntimeException(e);
        } finally {
            //关闭资源
            JdbcManager.close(conn, null, null);
        }
    }
}
