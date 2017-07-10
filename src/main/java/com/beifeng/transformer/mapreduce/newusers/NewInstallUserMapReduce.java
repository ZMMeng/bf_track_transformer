package com.beifeng.transformer.mapreduce.newusers;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerBaseMapReduce;
import com.beifeng.transformer.mapreduce.TransformerBaseMapper;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BrowserDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.mapreduce.totalusers.TotalInstallUserCalculate;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * 自定义的统计新增用户的MapReduce类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class NewInstallUserMapReduce extends TransformerBaseMapReduce {

    public static class NewInstallUserMapper extends TransformerBaseMapper<StatsUserDimension,
            TimeOutputValue> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
        //Map输出键对象
        private StatsUserDimension mapOutputKey = new StatsUserDimension();
        //Map输出值对象
        private TimeOutputValue mapOutputValue = new TimeOutputValue();
        //用户基本信息分析模块的KPI维度信息
        private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
        //浏览器基本信息分析模块的KPI维度信息
        private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER
                .name);
        //默认的浏览器信息维度对象
        private BrowserDimension defaultBrowser = new BrowserDimension("", "");
        //
        private String uuid, serverTime, platform, browserName, browserVersion;
        //
        private long longOfTime;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            //从HBase表中读取UUID
            uuid = getUuid(value);
            //从HBase表中读取服务器时间
            serverTime = getServerTime(value);
            //从HBase表中读取平台信息
            platform = getPlatform(value);

            //过滤无效数据，UUID、服务器时间和平台信息有任意一个为空或者服务器时间非数字，则视该条记录为无效记录
            if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric
                    (serverTime.trim()) || StringUtils.isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("UUID、服务器时间以及平台信息不能为空，服务器时间必须是数字");
                return;
            }

            //将服务器时间字符串转化为时间戳
            longOfTime = Long.valueOf(serverTime.trim());
            //设置Map输出值对象timeOutputValue的属性值
            mapOutputValue.setId(uuid);
            mapOutputValue.setTimestamp(longOfTime);

            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
            //DateDimension dateDimensionOfMonth = DateDimension.buildDate(longOfTime, DateEnum.MONTH);
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
            //设置Map输出对象的date维度

            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);

            //写browser相关的信息
            //从HBase表中读取浏览器的名称以及版本号，这两个值可以为空
            browserName = getBrowserName(value);
            //System.out.println(browserName);
            browserVersion = getBrowserVersion(value);
            List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,
                    browserVersion);

            //Map输出
            for (PlatformDimension pf : platformDimensions) {
                //进行覆盖操作
                mapOutputKey.setBrowser(defaultBrowser);
                statsCommonDimension.setKpi(newInstallUserKpi);
                statsCommonDimension.setPlatform(pf);
                context.write(mapOutputKey, mapOutputValue);
                // 输出browser维度统计
                statsCommonDimension.setKpi(newInstallUserOfBrowserKpi);
                for (BrowserDimension bw : browserDimensions) {
                    mapOutputKey.setBrowser(bw);
                    context.write(mapOutputKey, mapOutputValue);
                }
            }
        }
    }

    public static class NewInstallUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,
            StatsUserDimension, MapWritableValue> {

        //Reduce输出值对象
        private MapWritableValue outputValue = new MapWritableValue();
        //
        private Set<String> unique = new HashSet<String>();

        @Override
        protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
                throws IOException, InterruptedException {
            //清空unique
            unique.clear();
            //开始计算UUID个数
            //将UUID添加到unique集合中，以便统计UUID的去重个数
            for (TimeOutputValue value : values) {
                unique.add(value.getId());
            }
            MapWritable map = new MapWritable();
            map.put(new IntWritable(-1), new IntWritable(unique.size()));
            outputValue.setValue(map);

            //设置KPI
            /*String kpiName = key.getStatsCommon().getKpi().getKpiName();
            if (KpiType.NEW_INSTALL_USER.name.equals(kpiName)) {
                //计算stats_user表中的新增用户
                outputValue.setKpi(KpiType.NEW_INSTALL_USER);
            } else if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)) {
                //计算stats_device_browser中的新增用户
                outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
            }*/
            outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

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
        job.setPartitionerClass(NewInstallUserPartitioner.class);
        //设置不进行推测执行
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
    }

    /**
     * 执行MapReduce Job之后运行的方法
     *
     * @param job MapReduce Job
     * @param error MapReduce Job运行过程中可能出现的异常
     * @throws IOException
     */
    @Override
    protected void afterRunJob(Job job, Throwable error) throws IOException {
        try {

            if (error == null && job.isSuccessful()) {
                //MapReduce Job运行没有异常，且运行成功，那么计算总用户
                new TotalInstallUserCalculate().calculateTotalUsers(conf);
            } else if (error == null) {
                //MapReduce Job运行没有异常，但是运行失败
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
        //只分析launch事件
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.getAlias())));
        //Map任务需要获取的列名
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, //事件
                EventLogConstants.LOG_COLUMN_NAME_UUID, //UUID
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, //服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, //平台信息
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, //浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION //浏览器版本号
        };
        //继续添加过滤器
        filterList.addFilter(getColumnFilter(columns));
        return filterList;
    }

    /**
     * 自定义分区类
     */
    public static class NewInstallUserPartitioner extends Partitioner<StatsUserDimension, TimeOutputValue> {

        public int getPartition(StatsUserDimension key, TimeOutputValue value, int
                numPartitions) {
            KpiType kpi = KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName());
            switch (kpi) {
                case NEW_INSTALL_USER:
                    return 0;
                case BROWSER_NEW_INSTALL_USER:
                    return 1;
                default:
                    throw new IllegalArgumentException("无法获取分区id，当前kpi:" + key.getStatsCommon().getKpi()
                            .getKpiName() + "，当前reducer个数:" + numPartitions);
            }
        }
    }

}
