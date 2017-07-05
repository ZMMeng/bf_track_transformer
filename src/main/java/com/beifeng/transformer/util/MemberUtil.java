package com.beifeng.transformer.util;

import com.beifeng.utils.JdbcManager;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 操作member_info的工具类
 * 主要是判断member id是否是正常的id以及是否是一个新的会员id
 * Created by Administrator on 2017/7/5.
 */
public class MemberUtil {

    //缓存memberId查询结果
    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            //最多保存10000条数据
            return this.size() > 10000;
        }
    };

    /**
     * 删除member_info表中，指定日期的数据
     * @param date
     * @param conn
     * @throws SQLException
     */
    public static void deleteMemberInfoByDate(String date, Connection conn) throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("delete from member_info where created=?");
            pstmt.setString(1, date);
            pstmt.execute();
        }finally {
            if(pstmt != null){
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    //nothing
                }
            }
        }
    }

    /**
     * 判断member id的格式是否正常
     * @param memberId
     * @return 正常返回true，否则返回false
     */
    public static boolean isValidateMemberId(String memberId){
        //先判断memberId是否非空
        if(StringUtils.isBlank(memberId)){
            //memberId为空，直接返回false
            return false;
        }
        //进行格式判断
        return memberId.matches("[0-9A-Za-z]{1,32}");
    }

    /**
     * 判断memberId是否是一个新的会员id
     * @param memberId 需要判断的memberId
     * @param conn 数据库连接
     * @return 是则返回true，否则返回false
     * @throws SQLException
     */
    public static boolean isNewMemberId(String memberId, Connection conn) throws SQLException {
        //首先判断memberId是否非空
        if(StringUtils.isBlank(memberId)){
            //memberId为空直接返回false
            return false;
        }

        //判断该memberId是否已进行数据库查询
        Boolean isCheck = cache.get(memberId);
        if(isCheck != null){
            //该memberId已进行数据库查询，则直接返回查询结果
            return isCheck.booleanValue();
        }

        //该memberId没有进行过数据库查询
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select member_id,last_visit_date from member_info where member_id=?";

        try {
            pstmt= conn.prepareStatement(sql);
            pstmt.setString(1, memberId);
            rs = pstmt.executeQuery();
            if(rs.next()){
                //在member_info表中有该条记录，表示不是新记录，直接返回false
                cache.put(memberId, false);
                return false;
            }
            //在member_info表中没有该条记录，表示是新的会员记录，返回true
            cache.put(memberId, true);
            return true;
        } finally{
            JdbcManager.close(pstmt, rs);
        }
    }
}
