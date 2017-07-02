package com.beifeng.transformer.mr.nu;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 自定义的计算新用户的MapReduce类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class NewInstallUserMapReduce extends Configured implements Tool {

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
                    .toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME)));

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
                    statsUserDimension.setBrowser(bf);
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

            //开始计算UUID个数
            for (TimeOutputValue value : values){
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

    public int run(String[] args) throws Exception {
        return 0;
    }
}
