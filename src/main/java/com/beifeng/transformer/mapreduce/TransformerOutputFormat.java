package com.beifeng.transformer.mapreduce;

import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.service.IDimensionConverter;
import com.beifeng.transformer.service.impl.DimensionConverterImpl;
import com.beifeng.utils.JdbcManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义输出到MySQL的OutputFormat类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class TransformerOutputFormat extends OutputFormat<BaseDimension,
        BaseStatsValueWritable> {

    private static final Logger logger = Logger.getLogger
            (TransformerOutputFormat.class);

    public RecordWriter<BaseDimension, BaseStatsValueWritable>
    getRecordWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        //获取Hadoop配置信息
        Configuration conf = context.getConfiguration();
        Connection conn = null;
        //创建操作dimension表的对象
        IDimensionConverter converter = new DimensionConverterImpl();
        try {
            //获取connection
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            //取消自动提交
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("获取数据库连接失败", e);
            throw new IOException("获取数据可连接失败", e);
        }
        return new TransformerRecordWriter(conn, conf, converter);
    }

    /**
     * 检测输出空间，输出到MySQL无需检测
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {

    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath
                (context), context);
    }

    /**
     * 自定义具体输出Writer
     */
    private class TransformerRecordWriter extends RecordWriter<BaseDimension,
            BaseStatsValueWritable> {

        //数据库连接
        private Connection conn;
        //配置
        private Configuration conf;
        //dimension表的对象
        private IDimensionConverter converter;
        //缓存kpi
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();
        //存储当前可提交的kpi以及其可提交的次数
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

        public TransformerRecordWriter(Connection conn, Configuration conf, IDimensionConverter converter) {
            this.conn = conn;
            this.conf = conf;
            this.converter = converter;
        }

        public void write(BaseDimension key, BaseStatsValueWritable value)
                throws IOException, InterruptedException {
            //判断key和value是否为空
            if (key == null || value == null) {
                //只要key和value有一个为null，则直接返回
                return;
            }

            try {
                //获取value对应的KPI值
                KpiType kpi = value.getKpi();
                PreparedStatement pstmt;
                //记录批量执行数
                int count = 1;
                //首先判断缓存中是否有该KPI值
                if (map.get(key) == null) {
                    //缓存中没有该KPI值
                    //使用KPI值进行区分，返回的sql保存在Hadoop的配置信息中
                    pstmt = conn.prepareStatement(conf.get(kpi.name));
                    //将KPI放入到缓存中
                    map.put(kpi, pstmt);
                } else {
                    //缓存中有该KPI值，直接从缓存中获取
                    pstmt = map.get(kpi);
                    //获取批量执行的次数
                    count = batch.get(kpi);
                    count++;
                }
                //批量次数的存储
                batch.put(kpi, count);

                //获取collector的类全名
                String collectorName = conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpi.name);
                //获取collector类的字节码对象
                Class<?> clazz = Class.forName(collectorName);
                //根据获得的字节码对象创建collector对象
                IOutputCollector collector = (IOutputCollector) clazz.newInstance();
                //执行统计数据插入
                collector.collect(conf, key, value, pstmt, converter);

                //判断批量执行数是否达到Hadoop配置信息中的配置批量执行数
                if (count % Integer.valueOf(conf.get(GlobalConstants.JDBC_BATCH_NUMBER, GlobalConstants
                        .DEFAULT_JDBC_BATCH_NUMBER)) == 0) {
                    //达到，执行批处理
                    pstmt.executeBatch();
                    //提交
                    conn.commit();
                    //提交之后，将该kpi对应的批处理计数删除
                    batch.remove(kpi);
                }

            } catch (Exception e) {
                logger.error("在writer中写数据出现异常", e);
                throw new IOException(e);
            }

        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> entry : map.entrySet()) {
                    entry.getValue().executeUpdate();
                }
            } catch (SQLException e) {
                logger.error("执行executeUpdate方法异常", e);
                throw new IOException(e);
            } finally {
                try {
                    //进行conn的提交
                    if (conn != null) {
                        conn.commit();
                    }
                } catch (Exception e) {
                    //nothing
                } finally {
                    //关闭PrepareStatement对象
                    for (Map.Entry<KpiType, PreparedStatement> entry : map.entrySet()) {
                        try {
                            entry.getValue().close();
                        } catch (SQLException e) {
                            //nothing
                        }
                    }
                    //关闭Connection对象
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            //nothing
                        }
                    }
                }
            }
        }
    }
}