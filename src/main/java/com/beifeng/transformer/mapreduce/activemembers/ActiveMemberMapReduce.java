package com.beifeng.transformer.mapreduce.activemembers;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 自定义的统计活跃会员的MapReduce类
 * Created by Administrator on 2017/7/4.
 */
public class ActiveMemberMapReduce extends TransformerBaseMapReduce {

    public static class ActiveMemberMapper extends TransformerBaseMapper<StatsUserDimension,
            TimeOutputValue> {

        //打印日志对象
        private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
        //Map输出的键
        private StatsUserDimension mapOutputKey = new StatsUserDimension();
        //Map输出的值
        private TimeOutputValue mapOutputValue = new TimeOutputValue();
        //用户基本信息分析模块的KPI维度信息
        private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.name);
        //浏览器基本信息分析模块的KPI维度信息
        private KpiDimension activeMemberOfBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.name);
        //
        private String memberId, serverTime, platform, browserName, browserVersion;
        //
        private long longOfTime;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {

            //从HBase表中读取memberID
            memberId = getMemberId(value);
            //从HBase表中读取服务器时间
            serverTime = getServerTime(value);
            //从HBase表中读取平台信息
            platform = getPlatform(value);

            //过滤无效数据，会员ID、服务器时间和平台信息有任意一个为空或者服务器时间非数字，则视该条记录为无效记录
            if (StringUtils.isBlank(memberId) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric
                    (serverTime.trim()) || StringUtils.isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("会员ID、服务器时间以及平台信息不能为空，服务器时间必须是数字");
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
                statsCommonDimension.setKpi(activeMemberKpi);
                statsCommonDimension.setPlatform(pf);
                context.write(mapOutputKey, mapOutputValue);
                for (BrowserDimension bf : browserDimensions) {
                    statsCommonDimension.setKpi(activeMemberOfBrowserKpi);
                    //由于需要进行clean操作，故将该值复制后填充
                    mapOutputKey.setBrowser(WritableUtils.clone(bf, context.getConfiguration()));
                    context.write(mapOutputKey, mapOutputValue);
                }
            }
        }
    }

    public static class ActiveMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue,
            StatsUserDimension, MapWritableValue> {

        //
        private Set<String> unique = new HashSet<String>();
        //Reduce输出的值
        private MapWritableValue outputValue = new MapWritableValue();

        @Override
        protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context)
                throws IOException, InterruptedException {

            try {
                //将会员ID添加到unique集合中，以便统计会员ID的去重个数
                for (TimeOutputValue value : values) {
                    unique.add(value.getId());
                }

                MapWritable map = new MapWritable();
                map.put(new IntWritable(-1), new IntWritable(unique.size()));
                outputValue.setValue(map);
                //设置KPI
                outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

                //进行输出
                context.write(key, outputValue);
            } finally {
                //清空
                unique.clear();
            }
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
        job.setNumReduceTasks(3);
        //设置分区类
        job.setPartitionerClass(ActiveMemberPartitioner.class);
        //设置不进行推测执行
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
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
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, //事件名称
                EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID, //会员ID
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, //服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, //平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, //浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION //浏览器版本号
        };
        //添加过滤器
        filterList.addFilter(getColumnFilter(columns));
        //只需要pageView事件
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.getAlias
                ())));
        return filterList;
    }

    /**
     * 自定义分区类
     */
    public static class ActiveMemberPartitioner extends Partitioner<StatsUserDimension, TimeOutputValue> {

        public int getPartition(StatsUserDimension key, TimeOutputValue value, int numPartitions) {
            KpiType kpi = KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName());
            switch (kpi) {
                case ACTIVE_MEMBER:
                    return 0;
                case BROWSER_ACTIVE_MEMBER:
                    return 1;
                default:
                    throw new IllegalArgumentException("无法获取分区id，当前kpi:" + key.getStatsCommon().getKpi()
                            .getKpiName() + "，当前reducer个数:" + numPartitions);
            }
        }
    }
}
