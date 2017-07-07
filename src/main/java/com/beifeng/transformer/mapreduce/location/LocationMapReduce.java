package com.beifeng.transformer.mapreduce.location;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerBaseMapReduce;
import com.beifeng.transformer.mapreduce.TransformerBaseMapper;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsLocationDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.LocationDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.LocationReducerOutputValue;
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
import java.util.*;

/**
 * 统计地域维度信息的MapReduce
 * Created by Administrator on 2017/7/7.
 */
public class LocationMapReduce extends TransformerBaseMapReduce {

    public static class LocationMapper extends TransformerBaseMapper<StatsLocationDimension,
            TextsOutputValue> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger(LocationMapper.class);
        //Map任务输出的键
        private StatsLocationDimension mapOutputKey = new StatsLocationDimension();
        //Map任务输出的值
        private TextsOutputValue mapOutputValue = new TextsOutputValue();
        //统计地域信息的KPI维度
        private KpiDimension locationKpi = new KpiDimension(KpiType.LOCATION.name);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            //输入记录数自增
            this.inputRecords++;
            //从HBase表中获取平台信息、服务器时间、UUID和会话ID
            String platform = this.getPlatform(value);
            String serverTime = this.getServerTime(value);
            String uuid = this.getUuid(value);
            String sessionId = this.getSessionId(value);

            //判断平台信息、服务器时间、UUID和会话ID是否为空，以及服务器时间是否是数字
            if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric
                    (serverTime) || StringUtils.isBlank(uuid) || StringUtils.isBlank(sessionId)) {
                //上述条件有一项满足，则过滤该记录
                logger.warn("UUID、服务器时间、会话ID以及平台信息不能为空，服务器时间必须是数字");
                this.filterRecords++;
                return;
            }

            //从HBase表中获取国家名、省份名和城市名
            String country = this.getCountry(value);
            String province = this.getProvince(value);
            String city = this.getCity(value);

            long longOfTime = Long.valueOf(serverTime.trim());

            //构建日期维度信息
            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
            //构建平台维度信息
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
            //构建地域维度信息
            List<LocationDimension> locationDimensions = LocationDimension.buildList(country, province, city);

            //输出
            mapOutputValue.setUuid(uuid);
            mapOutputValue.setSessionId(sessionId);
            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);
            statsCommonDimension.setKpi(locationKpi);
            for (PlatformDimension pf : platformDimensions) {
                statsCommonDimension.setPlatform(pf);
                for (LocationDimension lc : locationDimensions) {
                    mapOutputKey.setLocation(lc);
                    context.write(mapOutputKey, mapOutputValue);
                    //输出记录数自增
                    this.outputRecords++;
                }
            }
        }
    }

    public static class LocationReducer extends Reducer<StatsLocationDimension, TextsOutputValue,
            StatsLocationDimension, LocationReducerOutputValue> {

        //用于将UUID去重
        private Set<String> unique = new HashSet<String>();
        //用于计算每个会话所访问的页面数，其长度即为会话个数，访问页面数为1的会话个数即为跳出会话个数
        private Map<String, Integer> sessionsVisit = new HashMap<String, Integer>();
        //Reduce输出的值
        private LocationReducerOutputValue outputValue = new LocationReducerOutputValue();

        @Override
        protected void reduce(StatsLocationDimension key, Iterable<TextsOutputValue> values, Context
                context) throws IOException, InterruptedException {
            try {
                for (TextsOutputValue value : values) {
                    String uuid = value.getUuid();
                    String sessionId = value.getSessionId();

                    //将UUID添加到unique中
                    unique.add(uuid);
                    //将sessionId添加到sessionsVisit中
                    //判断该会话ID是否已在sessionsVisit中
                    if (sessionsVisit.containsKey(sessionId)) {
                        //已存在，表明该会话ID已经有访问过的数据
                        int count = sessionsVisit.get(sessionId);
                        count++;
                        sessionsVisit.put(sessionId, count);
                    } else {
                        //不存在，表明该会话ID为第一次访问
                        sessionsVisit.put(sessionId, 1);
                    }
                }

                //outputValue输出
                outputValue.setActiveUsers(unique.size());
                outputValue.setSessions(sessionsVisit.size());
                int bounceNumber = 0;
                for (Map.Entry<String, Integer> entry : sessionsVisit.entrySet()) {
                    //查看其值是否为1
                    if (entry.getValue().intValue() == 1) {
                        bounceNumber++;
                    }
                }
                outputValue.setBounceSessions(bounceNumber);
                outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

                //输出
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
                .LOG_COLUMN_NAME_SESSION_ID, EventLogConstants.LOG_COLUMN_NAME_COUNTRY, EventLogConstants
                .LOG_COLUMN_NAME_PROVINCE, EventLogConstants.LOG_COLUMN_NAME_CITY, EventLogConstants
                .LOG_COLUMN_NAME_EVENT_NAME};
        list.addFilter(this.getColumnFilter(columns));
        //只需要pageview事件
        list.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.getAlias
                ())));
        return super.fetchHbaseFilter();
    }
}
