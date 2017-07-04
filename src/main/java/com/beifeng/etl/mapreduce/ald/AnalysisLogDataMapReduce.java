package com.beifeng.etl.mapreduce.ald;

import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.etl.utils.LoggerUtil;
import com.beifeng.utils.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 自定义数据解析MapReduce类
 * Created by 蒙卓明 on 2017/7/1.
 */
public class AnalysisLogDataMapReduce extends Configured implements Tool {

    private Configuration conf = HBaseConfiguration.create();

    /**
     * 自定义数据解析的Map类
     */
    public static class AnalysisLogDataMapper extends Mapper<LongWritable,
            Text, NullWritable, Put> {
        //打印日志工具
        private static final Logger logger = Logger.getLogger
                (AnalysisLogDataMapReduce.class);
        //主要用于标记，方便查看过滤数据，分别为输入记录数、过滤记录数和输出记录数
        private int inputRecords, filterRecords, outputRecords;
        //列簇名的二进制数据
        private byte[] family = Bytes.toBytes(EventLogConstants
                .HBASE_COLUMN_FAMILY_NAME);
        //
        private CRC32 crc32 = new CRC32();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //输入记录数自增
            inputRecords++;
            logger.debug("Analyse data of " + value);

            try {
                //解析日志
                Map<String, String> clientInfo = LoggerUtil.handleLog(value.toString());
                if (clientInfo.isEmpty()) {
                    //如果日志解析结果为空，将该条记录过滤，过滤记录数自增，直接返回
                    filterRecords++;
                    return;
                }

                //获取事件名称
                String eventAliasName = clientInfo.get
                        (EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
                EventLogConstants.EventEnum event = EventLogConstants
                        .EventEnum.valueOfAlias(eventAliasName);
                switch (event) {
                    case LAUNCH:
                    case PAGEVIEW:
                    case CHARGREQUEST:
                    case CHARGESUCCESS:
                    case CHARGEREFUND:
                    case EVENT:
                        //处理事件
                        handleData(clientInfo, context, event);
                        break;
                    default:
                        //事件在上述事件范围内，将该条记录过滤，过滤记录数自增，直接返回
                        filterRecords++;
                        logger.warn("该事件无法进行解析，事件名称为" + eventAliasName);
                        return;
                }

            } catch (Exception e) {
                //处理过程出现异常，将该记录过滤，过滤记录数自增，直接返回
                filterRecords++;
                logger.error("处理数据发生异常，数据为：" + value, e);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            super.cleanup(context);
            logger.info("输入数据：" + inputRecords + "，输出数据：" + outputRecords + "，过滤数据：" + filterRecords);
        }

        /**
         * 具体处理数据的方法
         *
         * @param clientInfo
         * @param context
         * @param event
         * @throws IOException
         * @throws InterruptedException
         */
        private void handleData(Map<String, String> clientInfo, Context
                context, EventLogConstants.EventEnum event) throws IOException,
                InterruptedException {
            //获取UUID
            String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
            //获取会员ID
            String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
            //获取服务器时间
            String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
            //判断服务器时间是否为空
            if (StringUtils.isBlank(serverTime)) {
                //服务器时间为空，将该记录过滤，过滤记录数自增，直接返回
                filterRecords++;
                return;
            }
            //服务器时间不为空
            //删除浏览器信息
            clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
            //创建rowkey，规则为：服务器时间戳 + (UUID + 会员ID + 事件别名)的crc编码
            String rowkey = generateRowKey(uuid, memberId, serverTime, event
                    .getAlias());
            //创建Put对象
            Put put = new Put(Bytes.toBytes(rowkey));
            //将clientInfo中的所有信息写入到HBase表中
            for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
                if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                    //只有key和value同时不为空的情况下，才能写入到HBase表中
                    put.add(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
            }
            context.write(NullWritable.get(), put);
            //事件处理成功，输出记录数自增
            outputRecords++;
        }


        /**
         * 根据传入的四个参数创建rowkey，规则为：服务器时间戳 + (UUID + 会员ID + 事件别名)的crc编码
         *
         * @param uuid
         * @param memberId
         * @param serverTime
         * @param eventAliasName
         * @return
         */
        private String generateRowKey(String uuid, String memberId, String
                serverTime, String eventAliasName) {
            StringBuilder sb = new StringBuilder();
            sb.append(serverTime).append("_");
            crc32.reset();
            if (StringUtils.isNotBlank(uuid)) {
                crc32.update(uuid.getBytes());
            }
            if (StringUtils.isNotBlank(memberId)) {
                crc32.update(memberId.getBytes());
            }
            crc32.update(eventAliasName.getBytes());
            sb.append(crc32.getValue() % 100000000L);
            return sb.toString();
        }
    }

    /*@Override
    public void setConf(Configuration conf) {
        //this.conf = HBaseConfiguration.create(conf);
        this.conf = HBaseConfiguration.create();
    }*/

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        processArgs(conf, args);

        //设置Job
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        //设置Mapper
        job.setMapperClass(AnalysisLogDataMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        //设置Reducer配置
        //集群运行，打成jar包，要求addDependencyJars参数为默认值true
        /*TableMapReduceUtil.initTableReducerJob(EventLogConstants
                .HBASE_TABLE_NAME, null, job);*/
        //本地运行，要求addDependencyJars参数为false
        TableMapReduceUtil.initTableReducerJob(EventLogConstants
                .HBASE_TABLE_NAME, null, job, null, null, null, null, false);
        //将Reduce任务个数设为0
        job.setNumReduceTasks(0);

        //设置文件输入路径
        setJobInputPaths(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 设置Job的输入路径
     *
     * @param job
     */
    private void setJobInputPaths(Job job) {
        Configuration conf = job.getConfiguration();
        FileSystem fs = null;
        try {
            //获取HDFS
            fs = FileSystem.get(conf);
            String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMS);
            //更改文件格式
            Path inputPath = new Path("/user/hadoop/flume/nginx_logs/" +
                    TimeUtil.parseLong2String(TimeUtil.parseString2Long(date),
                            "MM/dd/"));
            //判断输入文件是否存在
            if (fs.exists(inputPath)) {
                //输入文件存在，设置Job的输入路径
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                //输入文件不存在，抛出文件不存在的运行时异常
                throw new RuntimeException("文件不存在" + inputPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("设置Job的MapReduce输入路径出现异常");
        } finally {
            //关闭HDFS
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                //nothing
            }
        }
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
