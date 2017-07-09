package com.beifeng.transformer.mapreduce.inbound;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerBaseMapReduce;
import com.beifeng.transformer.mapreduce.TransformerBaseMapper;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsInboundDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.InboundReducerOutputValue;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 统计外链分析中的相关活跃用户和总会话个数的MapReduce类
 * Created by 蒙卓明 on 2017/7/7.
 */
public class InboundMapReduce extends TransformerBaseMapReduce {

    public static class InboundMapper extends TransformerBaseMapper<StatsInboundDimension, TextsOutputValue> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger(InboundMapper.class);
        //Map输出的键
        private StatsInboundDimension mapOutputKey = new StatsInboundDimension();
        //Map输出的值
        private TextsOutputValue mapOutputValue = new TextsOutputValue();
        //统计外链分析的KPI
        private KpiDimension inboundKpi = new KpiDimension(KpiType.INBOUND.name);
        //存储外链URL及其对应id的Map集合
        private Map<String, Integer> inbounds = null;

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

            //从HBase表中读取平台信息、服务器时间、外链、UUID和会话ID
            String platform = this.getPlatform(value);
            String serverTime = this.getServerTime(value);
            String refererUrl = this.getRefererUrl(value);
            String uuid = this.getUuid(value);
            String sessionId = this.getSessionId(value);

            //过滤无效数据
            //判断平台信息、服务器时间、外链、UUID和会话ID是否为空，以及服务器时间是否为数字
            if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils
                    .isNumeric(serverTime.trim()) || StringUtils.isBlank(refererUrl) || StringUtils.isBlank
                    (uuid) || StringUtils.isBlank(sessionId)) {
                //上述条件只要有一个满足，则过滤该条记录
                logger.warn("平台信息、服务器时间、外链、UUID和会话ID不能为空，服务器时间必须是数字！");
                //过滤记录数自增
                this.filterRecords++;
                return;
            }

            //获取外链URL对应的外链id
            int inboundId = 0;
            try {
                inboundId = getInboundIdByHost(UrlUtil.getHost(refererUrl));
            } catch (Exception e) {
                logger.warn("获取外链URL对应的外链id出现异常", e);
                inboundId = 0;
            }

            //判断外链ID是否有效
            if (inboundId <= 0) {
                //此时外链URL为无效的外链，同样过滤该条记录
                logger.warn("该URL对应的不是外链：" + refererUrl);
                this.filterRecords++;
                return;
            }

            long longOfTime = Long.valueOf(serverTime);

            mapOutputValue.setUuid(uuid);
            mapOutputValue.setSessionId(sessionId);
            //构建日期维度对象
            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
            //构建平台维度对象
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);
            statsCommonDimension.setKpi(inboundKpi);

            //输出
            for (PlatformDimension pf : platformDimensions) {
                statsCommonDimension.setPlatform(pf);
                mapOutputKey.getInbound().setId(InboundDimensionService.ALL_OF_INBOUND_ID);
                context.write(mapOutputKey, mapOutputValue);
                this.outputRecords++;
                mapOutputKey.getInbound().setId(inboundId);
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
                return 0;
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

    public static class InboundReducer extends Reducer<StatsInboundDimension, TextsOutputValue,
            StatsInboundDimension, InboundReducerOutputValue> {

        //用于将UUID去重
        private Set<String> unique = new HashSet<String>();
        //用于计算每个会话所访问的页面数，其长度即为会话个数，
        private Set<String> sessionsVisit = new HashSet<String>();
        //Reduce输出的值
        private InboundReducerOutputValue outputValue = new InboundReducerOutputValue();

        @Override
        protected void reduce(StatsInboundDimension key, Iterable<TextsOutputValue> values, Context
                context) throws IOException, InterruptedException {
            try {
                for (TextsOutputValue value : values) {
                    String uuid = value.getUuid();
                    String sessionId = value.getSessionId();
                    unique.add(uuid);
                    sessionsVisit.add(sessionId);
                }

                outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
                outputValue.setSessions(sessionsVisit.size());
                outputValue.setActiveUsers(unique.size());

                context.write(key, outputValue);
            } finally {
                //清空操作
                unique.clear();
                sessionsVisit.clear();
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
                .LOG_COLUMN_NAME_SERVER_TIME, EventLogConstants.LOG_COLUMN_NAME_UUID, EventLogConstants
                .LOG_COLUMN_NAME_SESSION_ID, EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.LOG_COLUMN_NAME_REFERENCE_URL};
        list.addFilter(this.getColumnFilter(columns));
        //只需要pageview事件
        list.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.getAlias
                ())));
        return super.fetchHbaseFilter();
    }
}
