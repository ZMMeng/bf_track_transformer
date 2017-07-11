package com.beifeng.transformer.service.rpc.server;

import com.beifeng.transformer.model.dimension.basic.*;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.utils.JdbcManager;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 操作Demension表的具体实现类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class DimensionConverterImpl implements IDimensionConverter {

    //mysql jdbc四要素
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://hadoop:3306/report?useSSL=false";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    //日志打印对象
    private static final Logger logger = Logger.getLogger(DimensionConverterImpl.class);

    //缓存BaseDimension对象信息
    //键为BaseDimension对象的类信息+字段值(id除外)，值为BaseDimension对象的id属性值
    //由于缓存涉及大量的添加/删除操作，使用链表实现的HashMap，效率更高
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {

        private static final long serialVersionUID = 1L;

        /**
         * 重载该方法，表示当LinkedHashMap的大小超过500时进行删除
         * @param eldest
         * @return
         */
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            //nothing
        }
    }

    /**
     * 根据dimension的value值获取id
     *
     * @param dimension
     * @return 如果数据库中有直接返回，如果没有，则在数据库中插入后返回新的id
     * @throws IOException
     */
    public int getDimensionIdByValue(BaseDimension dimension) throws
            IOException {
        String cacheKey = DimensionConverterImpl.buildCacheKey(dimension);
        //判断缓存cache中是否有对应的id
        if (cache.containsKey(cacheKey)) {
            //缓存cache中有对应的id则直接返回
            return cache.get(cacheKey);
        }
        //数据库中没有对应的id，在数据库中插入后返回新的id
        Connection conn = null;
        try {
            //1. 在数据库中查找是否有对应的id，有则返回
            //2. 数据库中没有对应的id，则将dimension数据插入到数据库中，再获取对应的id
            //具体执行的sql数组
            String[] sqls = null;
            //判断dimension的类型，不同的类型，其sql数组页不同
            if (dimension instanceof DateDimension) {
                sqls = buildDateSql();
            } else if (dimension instanceof PlatformDimension) {
                sqls = buildPlatformSql();
            } else if (dimension instanceof BrowserDimension) {
                sqls = buildBrowserSql();
            } else if (dimension instanceof KpiDimension) {
                sqls = buildKpiSql();
            } else if (dimension instanceof LocationDimension) {
                sqls = buildLocationSql();
            } else if (dimension instanceof EventDimension) {
                sqls = buildEventSql();
            } else if (dimension instanceof CurrencyTypeDimension) {
                sqls = buildCurrencyTypeSql();
            } else if (dimension instanceof PaymentTypeDimension) {
                sqls = buildPaymentTypeSql();
            } else {
                throw new IOException("不支持此种dimension的id的获取：" + dimension.getClass());
            }

            //获取数据库连接
            conn = getConnection();

            int id;
            synchronized (this) {
                id = executeSql(conn, cacheKey, sqls, dimension);
            }
            return id;
        } catch (Exception e) {
            logger.error("操作数据库出现异常：", e);
            throw new IOException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //nothing
                }
            }
        }
    }

    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     * @throws SQLException 获取失败，抛出异常
     */
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }

    /**
     * 创建cache key
     *
     * @param dimension
     * @return
     */
    public static String buildCacheKey(BaseDimension dimension) {
        StringBuilder sb = new StringBuilder();

        //判断dimension的类型，根据其类型来创建cache key
        //规则是类型信息+字段值(id除外)
        if (dimension instanceof DateDimension) {
            //dimension的类型是DateDimension
            //类型信息
            sb.append("date_dimension");
            //强转
            DateDimension date = (DateDimension) dimension;
            //添加字段值(id除外)
            sb.append(date.getYear()).append(date.getSeason()).append(date.getMonth()).append(date.getWeek
                    ()).append(date.getDay()).append(date.getType());
        } else if (dimension instanceof PlatformDimension) {
            //dimension的类型是PlatformDimension
            //类型信息
            sb.append("platform_dimension");
            //强转
            PlatformDimension platform = (PlatformDimension) dimension;
            //添加字段值(id除外)
            sb.append(platform.getPlatformName());
        } else if (dimension instanceof BrowserDimension) {
            //dimension的类型是BrowserDimension
            //类型信息
            sb.append("browser_dimension");
            //强转
            BrowserDimension browser = (BrowserDimension) dimension;
            //添加字段值(id除外)
            sb.append(browser.getBrowserName()).append(browser.getBrowserVersion());
        } else if (dimension instanceof KpiDimension) {
            //dimension的类型是KpiDimension
            //类型信息
            sb.append("kpi_dimension");
            //强转
            KpiDimension kpi = (KpiDimension) dimension;
            //添加字段值(id除外)
            sb.append(kpi.getKpiName());
        } else if (dimension instanceof LocationDimension) {
            //dimension的类型是LocationDimension
            //类型信息
            sb.append("location_dimension");
            //强转
            LocationDimension location = (LocationDimension) dimension;
            //添加字段值(id除外)
            sb.append(location.getCountry());
            sb.append(location.getProvince());
            sb.append(location.getCity());
        } else if (dimension instanceof EventDimension) {
            //dimension的类型是EventDimension
            //类型信息
            sb.append("event_dimension");
            //强转
            EventDimension event = (EventDimension) dimension;
            //添加字段值(id除外)
            sb.append(event.getCategory());
            sb.append(event.getAction());
        } else if (dimension instanceof CurrencyTypeDimension){
            //dimension的类型是CurrencyTypeDimension
            //类型信息
            sb.append("currency_type_dimension");
            //强转
            CurrencyTypeDimension currencyType = (CurrencyTypeDimension) dimension;
            //添加字段值(id除外)
            sb.append(currencyType.getCurrencyName());
        } else if (dimension instanceof PaymentTypeDimension){
            //dimension的类型是PaymentTypeDimension
            //类型信息
            sb.append("payment_type_dimension");
            //强转
            PaymentTypeDimension paymentType = (PaymentTypeDimension) dimension;
            //添加字段值(id除外)
            sb.append(paymentType.getPaymentType());
        }

        //判断sb是否为空
        if (sb.length() == 0) {
            //sb为空，说明dimension的类型不在上述提到的几种类型之中，抛出异常
            throw new RuntimeException("无法创建指定dimension的cache key：" + dimension.getClass());
        }
        return sb.toString();
    }

    /**
     * @param pstmt
     * @param dimension
     */
    private void setArgs(PreparedStatement pstmt, BaseDimension dimension)
            throws SQLException {
        int i = 0;
        //判断dimension的类型，不同的类型设置不同的参数
        if (dimension instanceof DateDimension) {
            //dimension的类型是DateDimension
            //强转
            DateDimension date = (DateDimension) dimension;
            //设置参数
            pstmt.setInt(++i, date.getYear());
            pstmt.setInt(++i, date.getSeason());
            pstmt.setInt(++i, date.getMonth());
            pstmt.setInt(++i, date.getWeek());
            pstmt.setInt(++i, date.getDay());
            pstmt.setString(++i, date.getType());
            pstmt.setDate(++i, new Date(date.getCalender().getTime()));
        } else if (dimension instanceof PlatformDimension) {
            //dimension的类型是PlatformDimension
            //强转
            PlatformDimension platform = (PlatformDimension) dimension;
            //设置参数
            pstmt.setString(++i, platform.getPlatformName());
        } else if (dimension instanceof BrowserDimension) {
            //dimension的类型是BrowserDimension
            //强转
            BrowserDimension browser = (BrowserDimension) dimension;
            pstmt.setString(++i, browser.getBrowserName());
            pstmt.setString(++i, browser.getBrowserVersion());
        } else if (dimension instanceof KpiDimension) {
            //dimension的类型是KpiDimension
            //强转
            KpiDimension kpi = (KpiDimension) dimension;
            pstmt.setString(++i, kpi.getKpiName());
        } else if (dimension instanceof LocationDimension) {
            //dimension的类型是LocationDimension
            //强转
            LocationDimension location = (LocationDimension) dimension;
            pstmt.setString(++i, location.getCountry());
            pstmt.setString(++i, location.getProvince());
            pstmt.setString(++i, location.getCity());
        } else if (dimension instanceof EventDimension) {
            //dimension的类型是EventDimension
            //强转
            EventDimension event = (EventDimension) dimension;
            pstmt.setString(++i, event.getCategory());
            pstmt.setString(++i, event.getAction());
        } else if (dimension instanceof CurrencyTypeDimension){
            //dimension的类型是CurrencyTypeDimension
            //强转
            CurrencyTypeDimension currencyType = (CurrencyTypeDimension) dimension;
            pstmt.setString(++i, currencyType.getCurrencyName());
        } else if(dimension instanceof PaymentTypeDimension){
            //dimension的类型是PaymentTypeDimension
            //强转
            PaymentTypeDimension paymentType = (PaymentTypeDimension) dimension;
            pstmt.setString(++i, paymentType.getPaymentType());
        }
    }

    /**
     * 创建date dimension的相关sql语句
     *
     * @return
     */
    private String[] buildDateSql() {
        String querySql = "select id from dimension_date where year=? and season=? and month=? and week=? " +
                "and day=? and type=? and calendar=?;";
        String insertSql = "insert into dimension_date (year,season,month,week,day,type,calendar) values " +
                "(?,?,?,?,?,?,?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建platform dimension的相关sql语句
     *
     * @return
     */
    private String[] buildPlatformSql() {
        String querySql = "select id from dimension_platform where platform_name=?;";
        String insertSql = "insert into dimension_platform (platform_name) values (?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建browser dimension的相关sql语句
     *
     * @return
     */
    private String[] buildBrowserSql() {
        String querySql = "select id from dimension_browser where browser_name=? and browser_version=?;";
        String insertSql = "insert into dimension_browser (browser_name,browser_version) values (?,?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建kpi dimension的相关sql语句
     *
     * @return
     */
    private String[] buildKpiSql() {
        String querySql = "select id from dimension_kpi where kpi_name=?;";
        String insertSql = "insert into dimension_kpi (kpi_name) values (?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建location dimension的相关sql语句
     *
     * @return
     */
    private String[] buildLocationSql() {
        String querySql = "select id from dimension_location where country=? and province=? and city=?;";
        String insertSql = "insert into dimension_location (country,province,city) values (?,?,?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建event dimension的相关sql语句
     *
     * @return
     */
    private String[] buildEventSql() {
        String querySql = "select id from dimension_event where category=? and action=?;";
        String insertSql = "insert into dimension_event (category,action) values (?,?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建currency type dimension的相关sql语句
     *
     * @return
     */
    private String[] buildCurrencyTypeSql() {
        String querySql = "select id from dimension_currency_type where currency_name=?;";
        String insertSql = "insert into dimension_currency_type (currency_name) values (?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 创建payment type dimension的相关sql语句
     *
     * @return
     */
    private String[] buildPaymentTypeSql() {
        String querySql = "select id from dimension_payment_type where payment_type=?;";
        String insertSql = "insert into dimension_payment_type (payment_type) values (?);";
        return new String[]{querySql, insertSql};
    }

    /**
     * 具体执行sql的方法
     *
     * @param conn
     * @param cacheKey
     * @param sqls
     * @param dimension
     * @return
     */
    private int executeSql(Connection conn, String cacheKey, String[] sqls,
                           BaseDimension dimension) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(sqls[0]);
            setArgs(pstmt, dimension);
            rs = pstmt.executeQuery();
            //判断结果集中是否有值
            if (rs.next()) {
                //有值，即为dimension对应的id
                return rs.getInt(1);
            }
            //没有值，表示该dimension在数据库中不存在
            //将dimension插入数据库后，再查出对应的id
            //Statement.RETURN_GENERATED_KEYS参数表示将产生的主键返回
            pstmt = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            setArgs(pstmt, dimension);
            pstmt.executeUpdate();
            //获取插入数据后新产生的主键id
            rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                //返回自动生成的id
                return rs.getInt(1);
            }
        } finally {
            JdbcManager.close(null, pstmt, rs);
        }
        throw new RuntimeException("从数据库中获取id失败");
    }

    /**
     * 获取版本信息
     *
     * @param protocol
     * @param clientVersion
     * @return
     * @throws IOException
     */
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return IDimensionConverter.versionID;
    }

    /**
     * 获取注册信息，直接返回null即可
     *
     * @param protocol
     * @param clientVersion
     * @param clientMethodsHash
     * @return
     * @throws IOException
     */
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int
            clientMethodsHash) throws IOException {
        return null;
    }
}