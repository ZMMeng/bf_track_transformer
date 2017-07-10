package com.beifeng.transformer.mapreduce.newmembers;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerBaseMapReduce;
import com.beifeng.transformer.mapreduce.TransformerBaseMapper;
import com.beifeng.transformer.mapreduce.totalmembers.TotalMemberCalculate;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BrowserDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.util.MemberUtil;
import com.beifeng.utils.JdbcManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 统计新增会员的MapReduce
 * Created by Administrator on 2017/7/5.
 */
public class NewMemberMapReduce extends TransformerBaseMapReduce {

    public static class NewMemberMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {

        //打印日志对象
        private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
        //Map输出的键
        private StatsUserDimension mapOutputKey = new StatsUserDimension();
        //Map输出的值
        private TimeOutputValue mapOutputValue = new TimeOutputValue();
        //用户基本信息分析模块的KPI维度信息
        private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.name);
        //浏览器基本信息分析模块的KPI维度信息
        private KpiDimension newMemberOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.name);
        //数据库连接
        private Connection conn = null;
        //
        private String memberId, serverTime, platform, browserName, browserVersion;
        //
        private long longOfTime;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //进行初始化操作
            Configuration conf = context.getConfiguration();
            try {
                //进行初始化操作
                conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
                //删除指定日期的数据
                MemberUtil.deleteMemberInfoByDate(conf.get(GlobalConstants.RUNNING_DATE_PARAMS), conn);
            } catch (SQLException e) {
                logger.error("获取数据库连接出现异常", e);
                throw new IOException("连接数据库失败", e);
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {

            //从HBase表中读取memberID
            memberId = getMemberId(value);
            //判断memberId是否是第一次访问，即判断该memberId是否是新增会员
            try {
                if (!MemberUtil.isValidateMemberId(memberId) || !MemberUtil.isNewMemberId(memberId, conn)) {
                    logger.warn("member id不能为空，且必须是第一次访问该网站的会员id");
                    return;
                }
            } catch (SQLException e) {
                logger.error("查询" + memberId + "是否是新增会员时出现数据库异常", e);
                throw new IOException("查询" + memberId + "是否是新增会员时出现数据库异常", e);
            }

            //从HBase表中读取服务器时间
            serverTime = getServerTime(value);
            //从HBase表中读取平台信息
            platform = getPlatform(value);

            //过滤无效数据，会员ID、服务器时间和平台信息有任意一个为空或者服务器时间非数字，则视该条记录为无效记录
            if (StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim()) || StringUtils
                    .isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("服务器时间、平台信息不能为空，服务器时间必须是数字");
                return;
            }

            //将服务器时间字符串转化为时间戳
            longOfTime = Long.valueOf(serverTime.trim());
            //设置Map输出值对象timeOutputValue的属性值，只需要id
            mapOutputValue.setId(memberId);
            //构建日期维度信息
            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
            //构建平台维度信息
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

            //从HBase表中读取浏览器的名称以及版本号，这两个值可以为空
            browserName = getBrowserName(value);
            browserVersion = getBrowserVersion(value);
            //构建浏览器维度信息
            List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,
                    browserVersion);

            //Map输出
            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);
            for (PlatformDimension pf : platformDimensions) {
                //清空statsUserDimension中BrowserDimension的内容
                mapOutputKey.getBrowser().clean();
                statsCommonDimension.setKpi(newMemberKpi);
                statsCommonDimension.setPlatform(pf);
                context.write(mapOutputKey, mapOutputValue);
                for (BrowserDimension bf : browserDimensions) {
                    statsCommonDimension.setKpi(newMemberOfBrowserKpi);
                    //由于需要进行clean操作，故将该值复制后填充
                    mapOutputKey.setBrowser(WritableUtils.clone(bf, context.getConfiguration()));
                    context.write(mapOutputKey, mapOutputValue);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //关闭数据库连接
            JdbcManager.close(conn, null, null);
        }
    }

    public static class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue,
            StatsUserDimension, MapWritableValue> {

        //
        private Set<String> unique = new HashSet<String>();
        //Reduce输出的值
        private MapWritableValue outputValue = new MapWritableValue();
        //
        private MapWritable map = new MapWritable();

        @Override
        protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
                throws IOException, InterruptedException {

            //进行清空操作
            unique.clear();

            //将会员ID添加到unique集合中，以便统计会员ID的去重个数
            for (TimeOutputValue value : values) {
                unique.add(value.getId());
            }

            //输出memberId
            outputValue.setKpi(KpiType.INSERT_MEMBER_INFO);
            for (String id : unique) {
                map.put(new IntWritable(-1), new Text(id));
                outputValue.setValue(map);
                context.write(key, outputValue);
            }

            map.put(new IntWritable(-1), new IntWritable(unique.size()));
            outputValue.setValue(map);
            //设置KPI
            outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

            //进行输出
            context.write(key, outputValue);
        }
    }

    /**
     * 执行MapReduce Job之前运行的方法
     *
     * @param job
     * @throws IOException
     */
    @Override
    protected void beforeRunJob(Job job) throws IOException {
        super.beforeRunJob(job);
        // 每个统计维度一个reducer
        job.setNumReduceTasks(2);
        //设置分区类
        job.setPartitionerClass(NewMemberPartitioner.class);
        //设置不进行推测执行
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
    }

    /**
     * 执行MapReduce Job之后运行的方法
     *
     * @param job   MapReduce Job
     * @param error MapReduce Job运行过程中可能出现的异常
     * @throws IOException
     */
    @Override
    protected void afterRunJob(Job job, Throwable error) throws IOException {
        try {
            if (error == null && job.isSuccessful()) {
                //MapReduce Job运行过程中没有出现异常，且运行成功
                new TotalMemberCalculate().calculateTotalMembers(conf);
            } else if (error == null) {
                //MapReduce Job运行过程中没有出现异常，但运行失败
                throw new RuntimeException("MapReduce Job运行失败");
            }
        } catch (Exception e) {
            if (error != null) {
                error = e;
            }
            throw new IOException("调用afterRunJob产生异常", e);
        } finally {
            super.afterRunJob(job, error);
        }
    }

    /**
     * 获取HBase的过滤器
     *
     * @return 过滤器
     */
    @Override
    protected Filter fetchHbaseFilter() {
        //创建HBase的过滤器集合
        FilterList filterList = new FilterList();
        //添加过滤器

        //Map任务需要获取的列名
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID, //会员ID
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, //服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, //平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, //浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION //浏览器版本号
        };
        //添加过滤器
        filterList.addFilter(getColumnFilter(columns));
        return filterList;
    }

    /**
     * 自定义分区类
     */
    public static class NewMemberPartitioner extends Partitioner<StatsUserDimension, TimeOutputValue> {

        public int getPartition(StatsUserDimension key, TimeOutputValue value, int numPartitions) {
            KpiType kpi = KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName());
            switch (kpi) {
                case NEW_MEMBER:
                    return 0;
                case BROWSER_NEW_MEMBER:
                    return 1;
                default:
                    throw new IllegalArgumentException("无法获取分区id，当前kpi:" + key.getStatsCommon().getKpi()
                            .getKpiName() + "，当前reducer个数:" + numPartitions);
            }
        }
    }
}
