package com.beifeng.transformer.hive.orders;

import com.beifeng.common.GlobalConstants;
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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 获取订单信息的UDF
 * Created by Administrator on 2017/7/12.
 */
public class OrderInfoUDF extends UDF {
    //数据库连接
    private Connection conn = null;
    //配置对象
    private Configuration conf = null;
    //缓存
    private Map<String, InnerOrderInfo> cache = new LinkedHashMap<String, InnerOrderInfo>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, InnerOrderInfo> eldest) {
            return cache.size() > 100;
        }
    };

    private void init(){
        if(conf == null){
            conf = new Configuration();
            conf.addResource("transformer-env.xml");
        }
        try {
            if(conn == null) {
                conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            }
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
     * 根据订单ID和标志位，获取对应的订单值
     *
     * @param orderId
     * @param flag
     * @return
     * @throws IllegalArgumentException
     */
    public Text evaluate(Text orderId, Text flag) throws IllegalArgumentException {
        init();
        if (orderId == null || flag == null || StringUtils.isBlank(orderId.toString().trim()) ||
                StringUtils.isBlank(flag.toString().trim())) {
            throw new IllegalArgumentException("参数异常，订单id不能为空");
        }

        String order = orderId.toString();
        InnerOrderInfo info = fetchInnerOrderInfo(order);
        Text defaultValue = new Text(GlobalConstants.DEFAULT_VALUE);
        String str = flag.toString();
        if ("pl".equals(str)) {
            return info == null || StringUtils.isBlank(info.getPlatform()) ? defaultValue : new Text
                    (info.getPlatform());
        }
        if ("cut".equals(str)) {
            return info == null || StringUtils.isBlank(info.getCurrencyType()) ? defaultValue : new
                    Text(info.getCurrencyType());
        }
        if ("pt".equals(str)) {
            return info == null || StringUtils.isBlank(info.getPaymentType()) ? defaultValue : new Text
                    (info.getPaymentType());
        }
        throw new IllegalArgumentException("参数异常flag必须为(pl,cut,pt)中的一个，给定的是：" + flag);

    }

    /**
     * 根据订单ID，获取订单金额
     *
     * @param orderId
     * @return
     * @throws IllegalArgumentException
     */
    public IntWritable evaluate(Text orderId) throws IllegalArgumentException {
        if (orderId == null || StringUtils.isBlank(orderId.toString().trim())) {
            throw new IllegalArgumentException("参数异常，订单id不能为空");
        }

        String order = orderId.toString();

        InnerOrderInfo info = fetchInnerOrderInfo(order);
        return info == null ? new IntWritable(0) : new IntWritable(info.getAmount());
    }

    /**
     * 根据订单ID，获取订单的信息
     *
     * @param orderId
     * @return
     */
    private InnerOrderInfo fetchInnerOrderInfo(String orderId) {
        InnerOrderInfo info = cache.get(orderId);
        if (info != null) {
            return info;
        }
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        info = new InnerOrderInfo();

        try {
            pstmt = conn.prepareStatement("select order_id,platform,s_time,currency_type,payment_type," +
                    "amount from order_info where order_id=?");
            int i = 0;
            pstmt.setString(++i, orderId.toString().trim());
            rs = pstmt.executeQuery();
            if (rs.next()) {
                info.setOrderId(rs.getString("order_id"));
                info.setPlatform(rs.getString("platform"));
                info.setCurrencyType(rs.getString("currency_type"));
                info.setPaymentType(rs.getString("payment_type"));
                info.setsTime(rs.getLong("s_time"));
                info.setAmount(rs.getInt("amount"));
            }
            return info;
        } catch (SQLException e) {
            throw new RuntimeException("查询数据库时发生异常", e);
        } finally {
            JdbcManager.close(null, pstmt, rs);
        }
    }

    /**
     * 内部类
     */
    private static class InnerOrderInfo {
        private String OrderId;
        private String currencyType;
        private String paymentType;
        private String platform;
        private long sTime;
        private int amount;

        public InnerOrderInfo() {
        }

        public InnerOrderInfo(String orderId, String currencyType, String paymentType, String platform,
                              long sTime, int amount) {
            OrderId = orderId;
            this.currencyType = currencyType;
            this.paymentType = paymentType;
            this.platform = platform;
            this.sTime = sTime;
            this.amount = amount;
        }

        public String getOrderId() {
            return OrderId;
        }

        public void setOrderId(String orderId) {
            OrderId = orderId;
        }

        public String getCurrencyType() {
            return currencyType;
        }

        public void setCurrencyType(String currencyType) {
            this.currencyType = currencyType;
        }

        public String getPaymentType() {
            return paymentType;
        }

        public void setPaymentType(String paymentType) {
            this.paymentType = paymentType;
        }

        public String getPlatform() {
            return platform;
        }

        public void setPlatform(String platform) {
            this.platform = platform;
        }

        public long getsTime() {
            return sTime;
        }

        public void setsTime(long sTime) {
            this.sTime = sTime;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }
    }
}
