package com.beifeng.transformer.mapreduce.inbound.bounce;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerBaseMapReduce;
import com.beifeng.transformer.mapreduce.TransformerBaseMapper;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsInboundBounceDimension;
import com.beifeng.transformer.model.dimension.StatsInboundDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.reduce.InboundBounceReducerOutputValue;
import com.beifeng.transformer.service.impl.InboundDimensionService;
import com.beifeng.transformer.util.UrlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 统计外链信息中的跳出会话个数
 * Created by 蒙卓明 on 2017/7/8.
 */
public class InboundBounceMapReduce extends TransformerBaseMapReduce {

    public static class InboundBounceMapper extends TransformerBaseMapper<StatsInboundBounceDimension,
            IntWritable> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger(InboundBounceMapper.class);
        //Map输出的键
        private StatsInboundBounceDimension mapOutputKey = new StatsInboundBounceDimension();
        //Map输出的值
        private IntWritable mapOutputValue = new IntWritable();
        //存储外链URL及其对应id的Map集合
        private Map<String, Integer> inbounds = null;
        //统计外链分析的跳出会话数的KPI
        private KpiDimension inboundBounceKpi = new KpiDimension(KpiType.INBOUND_BOUNCE.name);
        //默认的inboundId，用于标识不是外链
        public static final int DEFAULT_INBOUND_ID = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //获取inbound相关数据
            inbounds = InboundDimensionService.getInboundByType(context.getConfiguration(), 0);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            //输入记录数自增
            this.inputRecords++;

            //获取平台信息、服务器时间、外链、以及会话id
            String platform = this.getPlatform(value);
            String serverTime = this.getServerTime(value);
            String refererUrl = this.getRefererUrl(value);
            String sessionId = this.getSessionId(value);

            //过滤无效数据
            //判断平台信息、服务器时间、外链、UUID和会话ID是否为空，以及服务器时间是否为数字
            if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils
                    .isNumeric(serverTime.trim()) || StringUtils.isBlank(refererUrl) || StringUtils.isBlank
                    (sessionId)) {
                //上述条件只要有一个满足，则过滤该条记录
                logger.warn("平台信息、服务器时间、外链、UUID和会话ID不能为空，服务器时间必须是数字！");
                //过滤记录数自增
                this.filterRecords++;
                return;
            }

            long longOfTime = Long.valueOf(serverTime);
            //构建日期维度对象
            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
            //构建平台维度对象
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
            //构建外链维度对象
            int inboundId = DEFAULT_INBOUND_ID;
            try {
                inboundId = getInboundIdByHost(UrlUtil.getHost(refererUrl));
            } catch (Exception e) {
                logger.warn("获取外链URL对应的外链id出现异常", e);
                inboundId = DEFAULT_INBOUND_ID;
            }

            //设置输出的值
            mapOutputValue.set(inboundId);
            //设置输出的键
            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);
            statsCommonDimension.setKpi(inboundBounceKpi);
            mapOutputKey.setSessionId(sessionId);
            mapOutputKey.setServerTime(longOfTime);
            //输出
            for (PlatformDimension pf : platformDimensions) {
                statsCommonDimension.setPlatform(pf);
                context.write(mapOutputKey, mapOutputValue);
                this.outputRecords++;
            }
        }

        /**
         * 根据URL的host来获取不同的外链的id值
         *
         * @param host
         * @return 如果host不属于外链，返回0
         */
        private int getInboundIdByHost(String host) {
            if (!UrlUtil.isValidateInboundHost(host)) {
                return DEFAULT_INBOUND_ID;
            }
            //先假设该host对应的是其他外链
            int id = InboundDimensionService.OTHER_OF_INBOUND_ID;
            //查看该host对应的具体id
            for (Map.Entry<String, Integer> entry : inbounds.entrySet()) {
                String url = entry.getKey();
                if (host.equals(url) || host.startsWith(url) || host.matches(url)) {
                    id = entry.getValue();
                    break;
                }
            }
            return id;
        }
    }

    public static class InboundBounceReducer extends Reducer<StatsInboundBounceDimension, IntWritable,
            StatsInboundDimension, InboundBounceReducerOutputValue> {

        //Reduce输出的键
        private StatsInboundDimension outputKey = new StatsInboundDimension();

        @Override
        protected void reduce(StatsInboundBounceDimension key, Iterable<IntWritable> values, Context
                context) throws IOException, InterruptedException {
            //前一条记录的会话ID
            String preSessionId = "";
            //当前会话ID
            String curSessionId = "";
            //当前inboundId
            int curInboundId = InboundBounceMapper.DEFAULT_INBOUND_ID;
            //前一条记录的inboundId
            int preInboundId = InboundBounceMapper.DEFAULT_INBOUND_ID;
            //表示会话是一个跳出会话
            boolean isBounceVisit = true;
            //表示是一个新会话
            boolean isNewSession = true;

            Map<Integer, InboundBounceReducerOutputValue> map = new HashMap<Integer,
                    InboundBounceReducerOutputValue>();
            //给一个all维度的默认值
            map.put(InboundDimensionService.ALL_OF_INBOUND_ID, new InboundBounceReducerOutputValue(0));
            for (IntWritable value : values) {
                curSessionId = key.getSessionId();
                curInboundId = value.get();

                //判断是否是同一个会话，而且当前inboundId为0，一定是一个非跳出的会话
                if (curSessionId.equals(preSessionId) && (curInboundId == InboundBounceMapper
                        .DEFAULT_INBOUND_ID)) {
                    //该会话不是跳出会话
                    isBounceVisit = false;
                    continue;
                }

                //1. 是同一个会话，但是当前inboundId为0，或者是新的inboundId
                //2. 是新的会话

                //表示是一个新的会话或者是一个新的inbound，检查上一个inbound是否是一个跳出的inbound
                if (preInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID && isBounceVisit) {
                    //针对上一个inbound id需要进行一次跳出会话更新操作
                    map.get(preInboundId).incrBounceSessions();

                    //针对all维度的跳出会话个数计算
                    //一次会话中只要出现一次跳出外链，就将其算到all维度的跳出会话中去
                    if (!curSessionId.equals(preSessionId)) {
                        map.get(InboundDimensionService.ALL_OF_INBOUND_ID).incrBounceSessions();
                    }
                }

                //会话结束或者是当前inboundId不为0，而且与前一个inboundId不相等
                isBounceVisit = true;
                preInboundId = InboundBounceMapper.DEFAULT_INBOUND_ID;

                //如果当前inbound是一个新的inbound
                if (curInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID) {
                    preInboundId = curInboundId;
                    //更新操作
                    InboundBounceReducerOutputValue ibrov = map.get(curInboundId);
                    if (ibrov == null) {
                        ibrov = new InboundBounceReducerOutputValue(0);
                        map.put(curInboundId, ibrov);
                    }
                }

                //如果是新的会话，就更新会话
                if (!preSessionId.equals(curSessionId)) {
                    isNewSession = true;
                    preSessionId = curSessionId;
                }else{
                    isNewSession = false;
                }
            }

            //单独处理最后一条数据
            //表示是一个新的会话或者是一个新的inbound，检查上一个inbound是否是一个跳出的inbound
            if (preInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID && isBounceVisit) {
                //针对上一个inbound id需要进行一次跳出会话更新操作
                map.get(preInboundId).incrBounceSessions();

                //针对all维度的跳出会话个数计算
                //一次会话中只要出现一次跳出外链，就将其算到all维度的跳出会话中去
                if (isNewSession) {
                    map.get(InboundDimensionService.ALL_OF_INBOUND_ID).incrBounceSessions();
                }
            }

            //输出
            outputKey.setStatsCommon(StatsCommonDimension.clone(key.getStatsCommon()));
            for (Map.Entry<Integer, InboundBounceReducerOutputValue> entry : map.entrySet()) {
                InboundBounceReducerOutputValue outputValue = entry.getValue();
                outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
                outputKey.getInbound().setId(entry.getKey());
                context.write(outputKey, outputValue);
            }
        }
    }

    /**
     * 获取HBase的过滤器
     *
     * @return
     */
    @Override
    protected Filter fetchHbaseFilter() {
        FilterList list = new FilterList();
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_PLATFORM, EventLogConstants
                .LOG_COLUMN_NAME_SERVER_TIME, EventLogConstants.LOG_COLUMN_NAME_SESSION_ID,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, EventLogConstants
                .LOG_COLUMN_NAME_REFERENCE_URL};
        list.addFilter(this.getColumnFilter(columns));
        //只需要pageview事件
        list.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.getAlias
                ())));
        return super.fetchHbaseFilter();
    }

    @Override
    protected void beforeRunJob(Job job) throws IOException {
        super.beforeRunJob(job);
        //设置自定义分组类
        job.setGroupingComparatorClass(InboundBounceSecondSort.InboundBounceGroupingComparator.class);
        //设置自定义分区类
        job.setPartitionerClass(InboundBounceSecondSort.InboundBoucePartitioner.class);
    }
}
