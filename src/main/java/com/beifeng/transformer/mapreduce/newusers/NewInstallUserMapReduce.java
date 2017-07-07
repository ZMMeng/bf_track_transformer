package com.beifeng.transformer.mapreduce.newusers;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * 自定义的统计新增用户的MapReduce类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class NewInstallUserMapReduce extends Configured implements Tool {

    //HBase配置信息
    private static final Configuration conf = HBaseConfiguration.create();

    public static class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
        //Map输出键对象
        private StatsUserDimension mapOutputKey = new StatsUserDimension();
        //Map输出值对象
        private TimeOutputValue mapOutputValue = new TimeOutputValue();
        //HBase表列簇的二进制数据
        private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY_NAME);
        //用户基本信息分析模块的KPI维度信息
        private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
        //浏览器基本信息分析模块的KPI维度信息
        private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER
                .name);
        //默认的浏览器信息维度对象
        private BrowserDimension defaultBrowser = new BrowserDimension("", "");

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            //从HBase表中读取UUID
            String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_UUID)));
            //从HBase表中读取服务器时间
            String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_SERVER_TIME)));
            //从HBase表中读取平台信息
            String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_PLATFORM)));

            //过滤无效数据，UUID、服务器时间和平台信息有任意一个为空或者服务器时间非数字，则视该条记录为无效记录
            if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric
                    (serverTime.trim()) || StringUtils.isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("UUID、服务器时间以及平台信息不能为空，服务器时间必须是数字");
                return;
            }

            //将服务器时间字符串转化为时间戳
            long longOfTime = Long.valueOf(serverTime.trim());
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
            String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_BROWSER_NAME)));
            //System.out.println(browserName);
            String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_BROWSER_VERSION)));
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

        //本地运行
        TableMapReduceUtil.initTableMapperJob(initScan(job), NewInstallUserMapper.class, StatsUserDimension
                .class, TimeOutputValue.class, job, false);
        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        job.setOutputFormatClass(TransformerOutputFormat.class);
        //执行MapReduce Job
        if (job.waitForCompletion(true)) {
            //如果MapReduce Job执行成功，则计算总用户数
            new TotalInstallUserCalculate().calculateTotalUsers(conf);
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * 初始化Scan集合
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
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.getAlias())));
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
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
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
