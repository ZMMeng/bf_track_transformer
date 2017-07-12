package com.beifeng.transformer.hive.orders;

import com.beifeng.common.DateEnum;
import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.utils.JdbcManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/7/12.
 */
public class OrderTotalAmountUDF extends UDF {
    private Connection conn = null;

    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 100;
        }
    };

    public OrderTotalAmountUDF() {
        Configuration conf = new Configuration();
        conf.addResource("transformer-env.xml");
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
        } catch (SQLException e) {
            throw new RuntimeException("创建MySQL连接异常", e);
        }

        //添加一个钩子进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                JdbcManager.close(conn, null, null);
            }
        }));
    }

    /**
     * 根据给定的flag参数决定获取当前维度下的total amount值，
     *
     * @param platformDimensionId
     * @param dateDimensionId
     * @param currencyTypeDimensionId
     * @param paymentTypeDimensionId
     * @param amount
     * @param flag
     * @return 如果flag给定为revenue，那么获取总的成功支付订单金额，如果flag给定为refund，那么获取退款订单总金额
     */
    public IntWritable evaluate(IntWritable platformDimensionId, IntWritable dateDimensionId, IntWritable
            currencyTypeDimensionId, IntWritable paymentTypeDimensionId, IntWritable amount, Text flag) {
        if (platformDimensionId == null || dateDimensionId == null || currencyTypeDimensionId == null ||
                paymentTypeDimensionId == null || amount == null || flag == null || StringUtils.isBlank
                (flag.toString().trim())) {
            throw new IllegalArgumentException("参数异常，订单id不能为空");
        }

        String str = flag.toString();
        if ("revenue".equals(str)) {
            return new IntWritable(evaluateTotalRevenueAmount(platformDimensionId.get(), dateDimensionId
                    .get(), currencyTypeDimensionId.get(), paymentTypeDimensionId.get(), amount.get()));
        }
        if ("refund".equals(str)) {
            return new IntWritable(evaluateTotalRefundAmount(platformDimensionId.get(), dateDimensionId
                    .get(), currencyTypeDimensionId.get(), paymentTypeDimensionId.get(), amount.get()));
        }

        throw new IllegalArgumentException("参数异常flag必须为(revenue, refund)中的一个，给定的是：" + flag);
    }

    /**
     * 计算总的成功订单金额
     *
     * @param platformDimensionId
     * @param dateDimensionId
     * @param currencyTypeDimensionId
     * @param paymentTypeDimensionId
     * @param amount
     * @return
     */
    private int evaluateTotalRevenueAmount(int platformDimensionId, int dateDimensionId, int
            currencyTypeDimensionId, int paymentTypeDimensionId, int amount) {

        String key = platformDimensionId + "_" + dateDimensionId + "_" + currencyTypeDimensionId + "_" +
                paymentTypeDimensionId + "_revenue";
        Integer lastTotal = cache.get(key);
        if (lastTotal != null) {
            return amount;
        }

        PreparedStatement pstmt;
        ResultSet rs;

        try {
            //1. 获取当前日期的前一天的id
            int lastDateDimensionId = fetchLastDateDimensionId(dateDimensionId);
            //2. 前一天日期的id不是-1，那么获取对应的总订单金额
            int lastTotalRevenueAmount = 0;
            if (lastDateDimensionId != -1) {
                pstmt = conn.prepareStatement("select total_revenue_amount from stats_orders where " +
                        "platform_dimension_id=? and date_dimension_id=? and currency_type_dimension_id=? " +
                        "and payment_type_dimension_id=?");
                int i = 0;
                pstmt.setInt(++i, platformDimensionId);
                pstmt.setInt(++i, lastDateDimensionId);
                pstmt.setInt(++i, currencyTypeDimensionId);
                pstmt.setInt(++i, paymentTypeDimensionId);
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    lastTotalRevenueAmount = rs.getInt("total_revenue_amount");
                }
            }
            //3. 当天的总成功订单金额 = 前一天的总成功订单金额 + 当天的成功订单金额
            lastTotal = lastTotalRevenueAmount;
            cache.put(key, lastTotal);
            return lastTotal + amount;
        } catch (Exception e) {
            throw new RuntimeException("获取总支付成功订单金额出现异常", e);
        }
    }

    /**
     * 计算总的退款订单金额
     *
     * @param platformDimensionId
     * @param dateDimensionId
     * @param currencyTypeDimensionId
     * @param paymentTypeDimensionId
     * @param amount
     * @return
     */
    private int evaluateTotalRefundAmount(int platformDimensionId, int dateDimensionId, int
            currencyTypeDimensionId, int paymentTypeDimensionId, int amount) {

        String key = platformDimensionId + "_" + dateDimensionId + "_" + currencyTypeDimensionId + "_" +
                paymentTypeDimensionId + "_refund";

        Integer lastTotal = cache.get(key);
        if (lastTotal != null) {
            return amount;
        }

        PreparedStatement pstmt;
        ResultSet rs;


        try {
            //1. 获取当前日期的前一天的id
            int lastDateDimensionId = fetchLastDateDimensionId(dateDimensionId);
            //2. 前一天日期的id不是-1，那么获取对应的总订单金额
            int lastTotalRefundAmount = 0;
            if (lastDateDimensionId != -1) {
                pstmt = conn.prepareStatement("select total_refund_amount from stats_orders where " +
                        "platform_dimension_id=? and date_dimension_id=? and currency_type_dimension_id=? " +
                        "and payment_type_dimension_id=?");
                int i = 0;
                pstmt.setInt(++i, platformDimensionId);
                pstmt.setInt(++i, lastDateDimensionId);
                pstmt.setInt(++i, currencyTypeDimensionId);
                pstmt.setInt(++i, paymentTypeDimensionId);
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    lastTotalRefundAmount = rs.getInt("total_refund_amount");
                }
            }
            //3. 当天的总退款订单金额 = 前一天的总退款订单金额 + 当天的退款订单金额
            lastTotal = lastTotalRefundAmount;
            cache.put(key, lastTotal);
            return lastTotalRefundAmount + amount;
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return 0;
    }

    /**
     * 获取当天的前一天对应的id
     *
     * @param dateDimensionId
     * @return
     * @throws SQLException
     */
    private int fetchLastDateDimensionId(int dateDimensionId) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Date calendar = null;
        DateEnum de = null;

        //获取指定id的日期和类型
        try {
            pstmt = conn.prepareStatement("select calendar,`type` from dimension_date where id=?");
            pstmt.setInt(1, dateDimensionId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                calendar = rs.getDate("calendar");
                String type = rs.getString("type");
                de = DateEnum.valueOfName(type);
            }
        } finally {
            JdbcManager.close(null, pstmt, rs);
        }

        if (calendar == null || de == null) {
            throw new RuntimeException("无法从数据库中获取对应id的日期维度，给定id为：" + dateDimensionId);
        }

        //获取前一天的id
        //间隔毫秒
        long diff;
        switch (de) {
            case DAY:
                diff = GlobalConstants.MILLISECONDS_OF_DAY;
                break;
            default:
                throw new IllegalArgumentException("时间维度必须为day，当前为：" + de.name);
        }

        //创建前一天的日期维度对象
        DateDimension lastDateDimension = DateDimension.buildDate(calendar.getTime() - diff, de);

        try {
            pstmt = conn.prepareStatement("select id from dimension_date where `year`=? and season=? and " +
                    "`month`=? and week=? and `day`=? and `type`=? and calendar=?");
            int i = 0;
            pstmt.setInt(++i, lastDateDimension.getYear());
            pstmt.setInt(++i, lastDateDimension.getSeason());
            pstmt.setInt(++i, lastDateDimension.getMonth());
            pstmt.setInt(++i, lastDateDimension.getWeek());
            pstmt.setInt(++i, lastDateDimension.getDay());
            pstmt.setString(++i, lastDateDimension.getType());
            pstmt.setDate(++i, new java.sql.Date(lastDateDimension.getCalender().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("id");
            }
            return -1;
        } finally {
            JdbcManager.close(null, pstmt, rs);
        }
    }
}
