package com.beifeng.transformer.mapreduce.pageview;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.mapreduce.TransformerBaseMapReduce;
import com.beifeng.transformer.mapreduce.TransformerBaseMapper;
import com.beifeng.transformer.mapreduce.TransformerOutputFormat;
import com.beifeng.transformer.model.dimension.StatsCommonDimension;
import com.beifeng.transformer.model.dimension.StatsUserDimension;
import com.beifeng.transformer.model.dimension.basic.BrowserDimension;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.model.dimension.basic.KpiDimension;
import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.utils.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 统计页面浏览的MapReduce
 * Created by Administrator on 2017/7/6.
 */
public class PageViewMapReduce extends TransformerBaseMapReduce {

    public static class PageViewMapper extends TransformerBaseMapper<StatsUserDimension, NullWritable> {

        //日志打印对象
        private static final Logger logger = Logger.getLogger(PageViewMapper.class);
        //Map输出键对象
        private StatsUserDimension mapOutputKey = new StatsUserDimension();
        //页面浏览的KPI维度信息
        private KpiDimension pageViewKpi = new KpiDimension(KpiType.PAGE_VIEW.name);
        //
        private String platform, serverTime, url, browserName, browserVersion;
        //
        private long longOfTime;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            //从HBase表中读取平台信息
            platform = getPlatform(value);
            //从HBase表中读取服务器时间
            serverTime = getServerTime(value);
            //从HBase表读取当前URL
            url = getCurrentUrl(value);

            //过滤无效数据，url、服务器时间和平台信息有任意一个为空或者服务器时间非数字，则视该条记录为无效记录
            if (StringUtils.isBlank(url) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric
                    (serverTime.trim()) || StringUtils.isBlank(platform)) {
                //上述变量只要有一个为空，直接返回
                logger.warn("url、服务器时间以及平台信息不能为空，服务器时间必须是数字");
                return;
            }

            //将服务器时间字符串转化为时间戳
            longOfTime = Long.valueOf(serverTime.trim());


            //创建平台维度信息
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
            //创建日期维度信息
            DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
            //创建浏览器维度信息
            //从HBase表中读取浏览器的名称以及版本号，这两个值可以为空
            browserName = getBrowserName(value);
            //System.out.println(browserName);
            browserVersion = getBrowserVersion(value);
            List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,
                    browserVersion);

            //输出
            StatsCommonDimension statsCommonDimension = mapOutputKey.getStatsCommon();
            statsCommonDimension.setDate(dateDimension);
            statsCommonDimension.setKpi(pageViewKpi);
            for (PlatformDimension pf : platformDimensions) {
                //清空statsUserDimension中BrowserDimension的内容
                mapOutputKey.getBrowser().clean();
                statsCommonDimension.setPlatform(pf);
                for (BrowserDimension bw : browserDimensions) {
                    //由于需要进行clean操作，故将该值复制后填充
                    mapOutputKey.setBrowser(WritableUtils.clone(bw, context.getConfiguration()));
                    //此处使用null作为Map任务的输出的值，因为只需要计算mapOutputKey的个数即可
                    //使用null可以减少Map任务和Reduce任务之间的网络传输量
                    context.write(mapOutputKey, NullWritable.get());
                }
            }

        }
    }

    public static class PageViewReducer extends Reducer<StatsUserDimension, NullWritable,
            StatsUserDimension, MapWritableValue> {

        //Reduce输出值对象
        private MapWritableValue outputValue = new MapWritableValue();
        //outputValue的value属性值
        private MapWritable map = new MapWritable();

        @Override
        protected void reduce(StatsUserDimension key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            int pvCount = 0;
            for (NullWritable value : values) {
                pvCount++;
            }

            //System.out.println(key.getBrowser().getBrowserName());
            //System.out.println(pvCount);

            map.put(new IntWritable(-1), new IntWritable(pvCount));
            //设置outputValue的kpi属性
            outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            //设置outputValue的value属性
            outputValue.setValue(map);

            //输出
            context.write(key, outputValue);
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
        //只分析pageview事件
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.getAlias
                ())));
        //Map任务需要获取的列名
        String[] columns = {EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, //事件名称
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, //服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, //平台
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, //浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, //浏览器版本号
                EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL //当前URL
        };
        //继续添加过滤器
        filterList.addFilter(getColumnFilter(columns));
        return filterList;
    }
}
