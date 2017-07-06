package com.beifeng.transformer.mapreduce.sessions;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BrowserDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.util.TimeChain;
import com.beifeng.utils.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
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
 * 统计会话个数和会话总长度
 * Created by Administrator on 2017/7/5.
 */
public class SessionsMapReduce extends Configured implements Tool {

    //日志打印对象
    private static final Logger logger = Logger.getLogger(SessionsMapReduce.class);
    //HBase配置信息
    private static final Configuration conf = HBaseConfiguration.create();

    public static class SessionsMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

        //打印日志对象
        private static final Logger logger = Logger.getLogger(SessionsMapper.class);
        //HBase表列簇的二进制数据
        private static final byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY_NAME);
        //Map输出的键
        private StatsUserDimension mapOutputKey = new StatsUserDimension();
        //Map输出的值
        private TimeOutputValue mapOutputValue = new TimeOutputValue();
        //用户基本信息分析模块的KPI维度信息
        private KpiDimension sessionsKpi = new KpiDimension(KpiType.SESSIONS.name);
        //浏览器基本信息分析模块的KPI维度信息
        private KpiDimension sessionsOfBrowserKpi = new KpiDimension(KpiType.BROWSER_SESSIONS.name);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            //从HBase表中读取Session ID
            String sessionId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_SESSION_ID)));
            //System.out.println(sessionId);
            //从HBase表中读取服务器时间
            String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_SERVER_TIME)));
            //从HBase表中读取平台信息
            String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_PLATFORM)));

            //过滤无效数据，Session ID、服务器时间和平台信息有任意一个为空或者服务器时间非数字，则视该条记录为无效记录
            if (StringUtils.isBlank(sessionId) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric
                    (serverTime.trim()) || StringUtils.isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("Session ID、服务器时间以及平台信息不能为空，服务器时间必须是数字");
                return;
            }

            //将服务器时间字符串转化为时间戳
            long longOfTime = Long.valueOf(serverTime.trim());

            //构建日期维度信息
            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);

            //构建平台维度信息
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

            //从HBase表中读取浏览器的名称以及版本号，这两个值可以为空
            String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_BROWSER_NAME)));
            String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants
                    .LOG_COLUMN_NAME_BROWSER_VERSION)));
            //构建浏览器维度信息
            List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,
                    browserVersion);

            //设置Map输出值对象timeOutputValue的属性值
            mapOutputValue.setId(sessionId);
            mapOutputValue.setTimestamp(longOfTime);

            //Map输出
            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);
            for (PlatformDimension pf : platformDimensions) {
                //清空statsUserDimension中BrowserDimension的内容
                mapOutputKey.getBrowser().clean();
                statsCommonDimension.setKpi(sessionsKpi);
                statsCommonDimension.setPlatform(pf);
                context.write(mapOutputKey, mapOutputValue);
                for (BrowserDimension bf : browserDimensions) {
                    statsCommonDimension.setKpi(sessionsOfBrowserKpi);
                    //由于需要进行clean操作，故将该值复制后填充
                    mapOutputKey.setBrowser(WritableUtils.clone(bf, context.getConfiguration()));
                    context.write(mapOutputKey, mapOutputValue);
                }
            }
        }
    }

    public static class SessionsReducer extends Reducer<StatsUserDimension, TimeOutputValue,
            StatsUserDimension, MapWritableValue> {

        //Reduce输出的值
        private MapWritableValue outputValue = new MapWritableValue();
        //存储每一个Session ID，以及其对应的最大服务器时间和最小服务器时间，其长度就是session个数
        private Map<String, TimeChain> timeChainMap = new HashMap<String, TimeChain>();
        //outputValue中的value属性值，对应sessions KPI和browser_session KPI
        private MapWritable map = new MapWritable();
        //用于计算hourly的会话个数和会话时长，key为24个小时，value为一个Map集合，该Map集合存储了这一小时内的所有会话
        //(以sessionId区分)，以及相应的最大最小服务器时间(方便计算该会话的时长)
        private Map<Integer, Map<String, TimeChain>> hourlyTimeChainMap = new HashMap<Integer, Map<String,
                TimeChain>>();
        //outputValue中的value属性值，对应hourly_sessions KPI，key为24个小时，value为该小时内的会话总数
        private MapWritable hourlySessionsMap = new MapWritable();
        //outputValue中的value属性值，对应hourly_sessions_length KPI，key为24个小时，value为该小时内所有会话的总长度
        private MapWritable hourlySessionsLengthMap = new MapWritable();


        /**
         * 初始化map、hourlyMap和hourlyTimeChainMap
         */
        private void startUp() {
            //将所有Map集合清空
            map.clear();
            hourlySessionsMap.clear();
            hourlySessionsLengthMap.clear();
            hourlyTimeChainMap.clear();
            //初始化hourlySessionsMap、hourlySessionsLengthMap和hourTimeChainMap
            for (int i = 0; i < 24; i++) {
                //实际就是向每个Map集合添加key，即每一个小时，此时value未定，只确定类型
                hourlySessionsMap.put(new IntWritable(i), new IntWritable(0));
                hourlySessionsLengthMap.put(new IntWritable(i), new IntWritable(0));
                hourlyTimeChainMap.put(i, new HashMap<String, TimeChain>());
            }
        }

        @Override
        protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
                throws IOException, InterruptedException {

            //初始化
            startUp();

            //获取KPI名称
            String kpiName = key.getStatsCommon().getKpi().getKpiName();
            if (KpiType.SESSIONS.name.equals(kpiName)) {
                //计算stats_user的sessions和sessions_length，同时处理stats_hourly
                handleSessions(key, values, context);

            } else if (KpiType.BROWSER_SESSIONS.equals(kpiName)) {
                //计算stats_device_browser的sessions和sessions_length，同时处理stats_hourly
                handleBrowserSessions(key, values, context);
            }

        }

        /**
         * 处理普通的会话分析，包括sessions、hourly_sessions和hourly_sessions_lengths三个KPI
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void handleSessions(StatsUserDimension key, Iterable<TimeOutputValue> values,
                                    Context context) throws IOException, InterruptedException {
            //将Session ID添加到unique集合中，以便统计Session ID的去重个数
            for (TimeOutputValue value : values) {
                //首先获取这一次访问的会话ID和服务器时间
                String sessionId = value.getId();
                long time = value.getTimestamp();

                //处理正常统计
                //获取Session ID对应的存储有最大最小服务器时间的TimeChain对象
                TimeChain chain = timeChainMap.get(value.getId());
                //判断该TimeChain对象是否非空
                if (chain == null) {
                    //如果为空，表示timeChainMap中尚未存储该Session ID对应的TimeChain对象，需新建TimeChain对象
                    chain = new TimeChain(value.getTimestamp());
                    timeChainMap.put(value.getId(), chain);
                    continue;
                }
                chain.addTime(value.getTimestamp());

                //处理hourly统计
                //根据服务器时间，得到对应的访问时间
                int hour = TimeUtil.getDateInfo(time, DateEnum.HOUR);
                //sessionInfoOfThisHour是这一小时内所有会话的最大最小服务器时间，以会话ID为key
                Map<String, TimeChain> sessionInfoOfThisHour = hourlyTimeChainMap.get(hour);
                //获取这一会话ID对应的存储该会话ID最大最小服务器的对象
                TimeChain hourlyChain = sessionInfoOfThisHour.get(sessionId);
                //判断hourlyChain是否为空(是否是第一次读取该会话ID)
                if (hourlyChain == null) {
                    //第一次读取该会话ID，此时hourlyChain是否为空，创建TimeChain对象
                    hourlyChain = new TimeChain(time);
                    //添加到存储这一小时内会话情况的Map集合
                    sessionInfoOfThisHour.put(sessionId, hourlyChain);
                    //在存储有所有24个小时会话情况的Map集合更新这一小时的会话情况
                    hourlyTimeChainMap.put(hour, sessionInfoOfThisHour);
                }
                //如果不是第一次读取该会话ID，将这一次访问的服务器添加到TimeChain对象中，更新时间
                hourlyChain.addTime(time);
            }

            //此时已获得24个小时内所有会话的最大最小服务器时间，开始计算hourly统计信息
            for (Map.Entry<Integer, Map<String, TimeChain>> entry : hourlyTimeChainMap.entrySet()) {
                //设置当前小时的会话个数
                hourlySessionsMap.put(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()
                        .size()));
                //开始统计当前小时的会话时长
                //preSessionLength存储该会话ID对应的会话时长
                int preSessionLength = 0;
                //计算每一小时内的会话总时长
                for (Map.Entry<String, TimeChain> en : entry.getValue().entrySet()) {
                    //获取间隔毫秒数
                    long tmp = en.getValue().getTimeOfMills();
                    if (tmp < 0 || tmp > 3600000) {
                        //会话时长小于0或者大于一个小时，过滤该条记录
                        continue;
                    }
                    preSessionLength += tmp;
                }
                //计算间隔秒数，毫秒数之差不足一秒，算一秒
                if (preSessionLength % 1000 == 0) {
                    preSessionLength /= 1000;
                }else{
                    preSessionLength = preSessionLength / 1000 + 1;
                }
                hourlySessionsLengthMap.put(new IntWritable(entry.getKey()), new IntWritable(preSessionLength));
            }

            //进行hourly sessions输出
            outputValue.setValue(hourlySessionsMap);
            //设置outputValue的kpi属性
            outputValue.setKpi(KpiType.HOURLY_SESSIONS);
            context.write(key, outputValue);

            //进行hourly sessions length输出
            outputValue.setValue(hourlySessionsLengthMap);
            //设置outputValue的kpi属性
            outputValue.setKpi(KpiType.HOURLY_SESSIONS_LENGTH);
            context.write(key, outputValue);

            //进行sessions输出
            //计算所有session的会话长度
            long lengthOfSessions = 0L;
            for (Map.Entry<String, TimeChain> entry : timeChainMap.entrySet()) {
                long tmp = entry.getValue().getTimeOfMills();
                if (tmp < 0 || tmp > GlobalConstants.MILLISECONDS_OF_DAY) {
                    //如果计算所得值小于0或者大于一天的毫秒数，直接过滤
                    continue;
                }
                lengthOfSessions += tmp;
            }
            //最后结果以秒数形式展现，毫秒数之差不足一秒，算一秒
            if(lengthOfSessions % 1000 == 0){
                lengthOfSessions /= 1000;
            }else{
                lengthOfSessions = lengthOfSessions / 1000 + 1;
            }

            //设置outputValue的value属性
            //将会话个数放入map集合
            map.put(new IntWritable(-1), new IntWritable(timeChainMap.size()));
            //将会话长度放入map集合
            map.put(new IntWritable(-2), new IntWritable((int) lengthOfSessions));
            outputValue.setValue(map);
            //设置outputValue的kpi属性
            outputValue.setKpi(KpiType.SESSIONS);

            //输出
            context.write(key, outputValue);
        }

        /**
         * 处理添加浏览器维度信息维度的会话分析
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void handleBrowserSessions(StatsUserDimension key, Iterable<TimeOutputValue> values,
                                           Context context) throws IOException, InterruptedException {

            //将Session ID添加到unique集合中，以便统计Session ID的去重个数
            for (TimeOutputValue value : values) {

                //处理正常统计
                //获取Session ID对应的存储有最大最小服务器时间的TimeChain对象
                TimeChain chain = timeChainMap.get(value.getId());
                //判断该TimeChain对象是否非空
                if (chain == null) {
                    //如果为空，表示timeChainMap中尚未存储该Session ID对应的TimeChain对象，需新建TimeChain对象
                    chain = new TimeChain(value.getTimestamp());
                    timeChainMap.put(value.getId(), chain);
                    continue;
                }
                chain.addTime(value.getTimestamp());
            }

            //计算所有session的会话长度
            long lengthOfSessions = 0L;
            for (Map.Entry<String, TimeChain> entry : timeChainMap.entrySet()) {
                long tmp = entry.getValue().getTimeOfMills();
                if (tmp < 0 || tmp > GlobalConstants.MILLISECONDS_OF_DAY) {
                    //如果计算所得值小于0或者大于一天的毫秒数，直接过滤
                    continue;
                }
                lengthOfSessions += tmp;
            }
            //最后结果以秒数形式展现
            lengthOfSessions /= 1000;

            //设置outputValue的value属性
            //将会话个数放入map集合
            map.put(new IntWritable(-1), new IntWritable(timeChainMap.size()));
            //将会话长度放入map集合
            map.put(new IntWritable(-2), new IntWritable((int) lengthOfSessions));
            outputValue.setValue(map);
            //设置outputValue的kpi属性
            outputValue.setKpi(KpiType.BROWSER_SESSIONS);

            //输出
            context.write(key, outputValue);
        }
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

        //创建并设置Job
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置HBase输入Mapper的参数
        //本地运行
        TableMapReduceUtil.initTableMapperJob(initScan(job), SessionsMapper.class, StatsUserDimension
                .class, TimeOutputValue.class, job, false);
        //设置Reduce相关参数
        job.setReducerClass(SessionsReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //设置输出的相关参数
        job.setOutputFormatClass(TransformerOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
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

        //Map任务需要获取的列名
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_SESSION_ID, EventLogConstants
                .LOG_COLUMN_NAME_SERVER_TIME, EventLogConstants.LOG_COLUMN_NAME_PLATFORM, EventLogConstants
                .LOG_COLUMN_NAME_BROWSER_NAME, EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION};
        //添加过滤器
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
     * @param conf
     * @param args
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