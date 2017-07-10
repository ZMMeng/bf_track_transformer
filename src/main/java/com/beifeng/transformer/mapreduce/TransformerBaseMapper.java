package com.beifeng.transformer.mapreduce;

import com.beifeng.common.EventLogConstants;
import com.sun.istack.internal.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * transformer相关MapReduce Job中Mapper的公用父类，主要提供计数和HBase value的获取
 * Created by 蒙卓明 on 2017/7/6.
 */
public class TransformerBaseMapper<KEYOUT, VLAUEOUT> extends TableMapper<KEYOUT, VLAUEOUT> {

    //日志打印信息
    private static final Logger logger = Logger.getLogger(TransformerBaseMapper.class);
    //MapReduce Job的Map任务开始运行时间的时间戳
    private long startTime = System.currentTimeMillis();
    //配置信息
    protected Configuration conf = null;
    //输入记录条数
    protected int inputRecords = 0;
    //过滤记录条数
    protected int filterRecords = 0;
    //输出记录条数
    protected int outputRecords = 0;
    //HBase表列簇的二进制数据
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY_NAME);

    /**
     * 初始化方法
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.conf = context.getConfiguration();
    }

    /**
     * 显示MapReduce Job的Map任务阶段完成的信息
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
            //MapReduce Job的Map任务运行结束时间的时间戳
            StringBuilder sb = new StringBuilder();
            long endTime = System.currentTimeMillis();
            //获取该MapReduce Job的Job id
            String jobId = context.getJobID().toString();
            sb.append("job_id：").append(jobId);
            sb.append("；startTime：").append(startTime);
            sb.append("；endTime：").append(endTime);
            sb.append("；useTime：").append((endTime - startTime)).append(" ms");
            sb.append("；inputRecords").append(inputRecords);
            sb.append("；filterRecords").append(filterRecords);
            sb.append("；outputRecords").append(outputRecords);

            System.out.println(sb.toString());
            logger.info(sb.toString());
        } catch (Exception e) {
            //nothing
        }
    }

    /**
     * 获取当前URL
     *
     * @param value
     * @return
     */
    public String getCurrentUrl(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL);
    }

    /**
     * 获取会员ID
     *
     * @param value
     * @return
     */
    public String getMemberId(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
    }

    /**
     * 获取UUID
     *
     * @param value
     * @return
     */
    public String getUuid(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_UUID);
    }

    /**
     * 获取平台名称
     *
     * @param value
     * @return
     */
    public String getPlatform(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_PLATFORM);
    }

    /**
     * 获取服务器时间戳
     *
     * @param value
     * @return
     */
    public String getServerTime(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
    }

    /**
     * 获取浏览器名称
     *
     * @param value
     * @return
     */
    public String getBrowserName(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME);
    }

    /**
     * 获取浏览器版本号
     *
     * @param value
     * @return
     */
    public String getBrowserVersion(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION);
    }

    /**
     * 获取国家名
     *
     * @param value
     * @return
     */
    public String getCountry(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_COUNTRY);
    }

    /**
     * 获取省份名
     *
     * @param value
     * @return
     */
    public String getProvince(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_PROVINCE);
    }

    /**
     * 获取城市名
     *
     * @param value
     * @return
     */
    public String getCity(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_CITY);
    }

    /**
     * 获取会话ID
     *
     * @param value
     * @return
     */
    public String getSessionId(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_SESSION_ID);
    }

    /**
     * 获取外链
     *
     * @param value
     * @return
     */
    public String getRefererUrl(Result value) {
        return fetchValue(value, EventLogConstants.LOG_COLUMN_NAME_REFERENCE_URL);
    }

    /**
     * 获取value中column列的值
     *
     * @param value
     * @param column
     * @return
     */
    private String fetchValue(Result value, String column) {
        return Bytes.toString(value.getValue(family, Bytes.toBytes(column)));
    }
}