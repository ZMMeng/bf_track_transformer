package com.beifeng.transformer.mr.nu;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.mr.TransformerOutputFormat;
import com.beifeng.utils.JdbcManager;
import com.beifeng.utils.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * 自定义的计算新增用户的MapReduce类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class NewInstallUserMapReduce extends Configured implements Tool {

    //日志打印对象
    private static final Logger logger = Logger.getLogger
            (NewInstallUserMapReduce.class);
    private static final Configuration conf = HBaseConfiguration.create();

    public static class NewInstallUserMapper extends
            TableMapper<StatsUserDimension, TimeOutputValue> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger
                (NewInstallUserMapper.class);
        //Map输出键对象
        private StatsUserDimension statsUserDimension = new
                StatsUserDimension();
        //Map输出值对象
        private TimeOutputValue timeOutputValue = new TimeOutputValue();
        //HBase表列簇的二进制数据
        private byte[] family = Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME);
        private KpiDimension newInstallUserKpi = new KpiDimension(KpiType
                .NEW_INSTALL_USER.name);
        private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension
                (KpiType.BROWSER_NEW_INSTALL_USER.name);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context
                context) throws IOException, InterruptedException {
            //从HBase表中读取UUID
            String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes
                    (EventLogConstants.LOG_COLUMN_NAME_UUID)));
            //从HBase表中读取服务器时间
            String serverTime = Bytes.toString(value.getValue(family, Bytes
                    .toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
            //从HBase表中读取平台信息
            String platform = Bytes.toString(value.getValue(family, Bytes
                    .toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));

            //判断UUID、serverTime以及platform是否为空
            if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime)
                    || StringUtils.isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("UUID、服务器时间以及平台信息不能为空");
                return;
            }

            //将服务器时间字符串转化为时间戳
            long longOfTime = Long.valueOf(serverTime);
            //设置Map输出值对象timeOutputValue的属性值
            timeOutputValue.setId(uuid);
            timeOutputValue.setTimestamp(longOfTime);

            DateDimension dateDimension = DateDimension.buildDate(longOfTime,
                    DateEnum.DAY);
            DateDimension dateDimensionOfMonth = DateDimension.buildDate
                    (longOfTime, DateEnum.MONTH);
            List<PlatformDimension> platformDimensions = PlatformDimension
                    .buildList(platform);
            //设置Map输出对象的date维度

            StatsCommonDimension statsCommonDimension = statsUserDimension
                    .getStatsCommon();
            statsCommonDimension.setDate(dateDimension);

            //写browser相关的信息
            //从HBase表中读取浏览器的名称以及版本号，这两个值可以为空
            String browserName = Bytes.toString(value.getValue(family, Bytes
                    .toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
            String browserVersion = Bytes.toString(value.getValue(family,
                    Bytes.toBytes(EventLogConstants
                            .LOG_COLUMN_NAME_BROWSER_VERSION)));
            List<BrowserDimension> browserDimensions = BrowserDimension
                    .buildList(browserName, browserVersion);

            //Map输出
            for (PlatformDimension pf : platformDimensions) {
                //清空statsUserDimension中BrowserDimension的内容
                statsUserDimension.getBrowser().clean();
                statsCommonDimension.setKpi(newInstallUserKpi);
                statsCommonDimension.setPlatform(pf);
                context.write(statsUserDimension, timeOutputValue);
                for (BrowserDimension bf : browserDimensions) {
                    statsCommonDimension.setKpi(newInstallUserOfBrowserKpi);
                    //由于需要进行clean操作，故将该值复制后填充
                    statsUserDimension.setBrowser(WritableUtils.clone(bf,
                            context.getConfiguration()));
                    context.write(statsUserDimension, timeOutputValue);
                }
            }
        }
    }

    public static class NewInstallUserReducer extends
            Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension,
                    MapWritableValue> {

        //Reduce输出值对象
        private MapWritableValue outputValue = new MapWritableValue();
        //
        private Set<String> unique = new HashSet<String>();

        @Override
        protected void reduce(StatsUserDimension key,
                              Iterable<TimeOutputValue> values, Context
                                      context) throws IOException,
                InterruptedException {

            //清空unique
            unique.clear();
            //开始计算UUID个数
            for (TimeOutputValue value : values) {
                unique.add(value.getId());
            }
            MapWritable map = new MapWritable();
            map.put(new IntWritable(-1), new IntWritable(unique.size()));
            outputValue.setValue(map);

            //获取KPI名称
            String kpiName = key.getStatsCommon().getKpi().getKpiName();
            if (KpiType.NEW_INSTALL_USER.name.equals(kpiName)) {
                //计算stats_user表中的新增用户
                outputValue.setKpi(KpiType.NEW_INSTALL_USER);
            } else if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)) {
                //计算stats_device_browser中的新增用户
                outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
            }

            context.write(key, outputValue);

        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public int run(String[] args) throws Exception {
        //获取HBase配置信息
        Configuration conf = this.getConf();
        //向conf添加关于输出到MySQL的配置信息
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");
        //处理参数
        processArgs(conf, args);

        //设置Job，各种类设置
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        /*job.setMapperClass(NewInstallUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);
        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);*/
        //本地运行
        TableMapReduceUtil.initTableMapperJob(initScan(job),
                NewInstallUserMapper
                        .class, StatsUserDimension.class, TimeOutputValue
                        .class, job, false);
        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        job.setOutputFormatClass(TransformerOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 计算总用户
     *
     * @param conf
     */
    private void calculateTotalUsers(Configuration conf) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        long date = TimeUtil.parseString2Long(conf.get(GlobalConstants
                .RUNNING_DATE_PARAMS));
        //获取当天的日期维度信息
        DateDimension todayDimension = DateDimension.buildDate(date, DateEnum
                .DAY);
        //获取前一天的日期维度信息
        DateDimension yesterdayDimension = DateDimension.buildDate(date -
                GlobalConstants.MILLISECONDS_OF_DAY, DateEnum.DAY);

        int yesterdayDimensionId = -1;
        int todayDimensionId = -1;

        //第一步：获取日期id
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants
                    .WAREHOUSE_OF_REPORT);
            //获取前一天的日期id
            pstmt = conn.prepareStatement("select id from dimension_date " +
                    "where year=? and season=? and month=? and week=? and " +
                    "day=? and type=? and calendar=?");
            int i = 0;
            pstmt.setInt(++i, yesterdayDimension.getYear());
            pstmt.setInt(++i, yesterdayDimension.getSeason());
            pstmt.setInt(++i, yesterdayDimension.getMonth());
            pstmt.setInt(++i, yesterdayDimension.getWeek());
            pstmt.setInt(++i, yesterdayDimension.getDay());
            pstmt.setString(++i, yesterdayDimension.getType());
            pstmt.setDate(++i, new Date(yesterdayDimension
                    .getCalender().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                yesterdayDimensionId = rs.getInt(1);
            }

            //获取当天的日期id
            pstmt = conn.prepareStatement("select id from dimension_date " +
                    "where year=? and season=? and month=? and week=? and " +
                    "day=? and type=? and calendar=?");
            i = 0;
            pstmt.setInt(++i, todayDimension.getYear());
            pstmt.setInt(++i, todayDimension.getSeason());
            pstmt.setInt(++i, todayDimension.getMonth());
            pstmt.setInt(++i, todayDimension.getWeek());
            pstmt.setInt(++i, todayDimension.getDay());
            pstmt.setString(++i, todayDimension.getType());
            pstmt.setDate(++i, new Date(todayDimension.getCalender().getTime
                    ()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                todayDimensionId = rs.getInt(1);
            }

            //第二步：获取前一天的原始数据，存储格式为dateid_platformid = totalusers
            Map<String, Integer> oldValueMap = new HashMap<String, Integer>();
            //开始更新stats_user
            if (yesterdayDimensionId > -1) {
                pstmt = conn.prepareStatement("select platform_dimension_id," +
                        "total_install_users from stats_user where " +
                        "date_dimension_id=?");
                pstmt.setInt(1, yesterdayDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalInstallUsers = rs.getInt("total_install_users");
                    oldValueMap.put("" + platformId, totalInstallUsers);
                }
            }

            //添加当天的新用户
            pstmt = conn.prepareStatement("select platform_dimension_id," +
                    "new_install_users from stats_user where " +
                    "date_dimension_id=?");
            pstmt.setInt(1, todayDimensionId);
            rs = pstmt.executeQuery();
            while(rs.next()){
                int platformId = rs.getInt("platform_dimension_id");
                int newInstallUsers = rs.getInt("new_install_users");
                if(oldValueMap.containsKey(platformId)){
                    newInstallUsers += oldValueMap.get("" + platformId);
                }
                oldValueMap.put("" + platformId, newInstallUsers);
            }

            //更新操作
            pstmt = conn.prepareStatement("insert into stats_user " +
                    "(platform_dimension_id,date_dimension_id," +
                    "total_install_users) values (?,?,?) on duplicate key " +
                    "update total_install_users=?");

            for(Map.Entry<String, Integer> entry : oldValueMap.entrySet()){
                pstmt.setInt(1, Integer.valueOf(entry.getKey()));
                pstmt.setInt(2, todayDimensionId);
                pstmt.setInt(3, entry.getValue());
                pstmt.setInt(4, entry.getValue());
                pstmt.execute();
            }

            // 开始更新stats_device_browser:TODO
            // 添加今天的总用户:TODO
        } catch (SQLException e) {

        }


    }

    /**
     * 初识化Scan集合
     *
     * @param job Scan集合所属的MapReduce Job
     * @return Scan集合
     */
    private List<Scan> initScan(Job job) {
        //获取HBase配置信息
        Configuration config = job.getConfiguration();
        //获取运行时间，格式为yyyy-MM-dd
        String date = config.get(GlobalConstants.RUNNING_DATE_PARAMS);
        //将日期字符串转换成时间戳格式
        //起始
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.MILLISECONDS_OF_DAY;
        //时间戳 + ...
        Scan scan = new Scan();
        //设定HBase扫描的起始rowkey和结束rowkey
        scan.setStartRow(Bytes.toBytes("" + startDate));
        scan.setStopRow(Bytes.toBytes("" + endDate));

        //创建HBase的过滤器集合
        FilterList filterList = new FilterList();
        //添加过滤器
        //只分析launch事件
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes
                (EventLogConstants.HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes
                (EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter
                .CompareOp.EQUAL, Bytes
                .toBytes
                        (EventLogConstants.EventEnum.LAUNCH.getAlias())));
        //Map任务需要获取的列名
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.LOG_COLUMN_NAME_UUID, EventLogConstants
                .LOG_COLUMN_NAME_SERVER_TIME, EventLogConstants
                .LOG_COLUMN_NAME_PLATFORM, EventLogConstants
                .LOG_COLUMN_NAME_BROWSER_NAME, EventLogConstants
                .LOG_COLUMN_NAME_BROWSER_VERSION};
        //继续添加过滤器
        filterList.addFilter(getColumnFilter(columns));
        //将过滤器添加到scan对象中
        scan.setFilter(filterList);
        //设置HBase表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes
                (EventLogConstants.HBASE_TABLE_NAME));
        return Lists.newArrayList(scan);
    }

    /**
     * 获取过滤给定列名的过滤器
     *
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        //返回根据列名前缀匹配的过滤器
        return new MultipleColumnPrefixFilter(filter);
    }

    /**
     * 处理参数
     *
     * @param conf HBase配置信息
     * @param args 参数
     */
    private void processArgs(Configuration conf, String[] args) {
        //时间格式字符串
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    //此时args[i]不是最后一个参数，从传入的参数中获取时间格式字符串
                    date = args[++i];
                    break;
                }
            }
        }

        //判断date是否为空，以及其是否为有效的时间格式字符串
        //要求date的格式为yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate
                (date)) {
            //date为空或者是无效的时间格式字符串，使用默认时间
            //默认时间是前一天
            date = TimeUtil.getYesterday();

        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMS, date);
    }
}
