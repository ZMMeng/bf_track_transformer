package com.beifeng.transformer.mapreduce;

import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.utils.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * transformer相关MapReduce Job中MapReduce的公用抽象父类，
 * Created by Administrator on 2017/7/7.
 */
public abstract class TransformerBaseMapReduce extends Configured implements Tool {

    //日志打印对象
    private static final Logger logger = Logger.getLogger(TransformerBaseMapReduce.class);
    //HBase配置信息
    protected static final Configuration conf = HBaseConfiguration.create();

    //MapReduce Job的名字
    private String jobName;
    private Class<?> mapReduceClass;
    private Class<? extends TableMapper<?, ?>> mapperClass;
    private Class<? extends WritableComparable<?>> mapOutputKeyClass;
    private Class<? extends Writable> mapOutputValueClass;
    private Class<? extends Reducer<?, ?, ?, ?>> reducerClass;
    private Class<?> outputKeyClass;
    private Class<?> outputValueClass;
    private Class<? extends OutputFormat<?, ?>> outputFormatClass;
    private long startTime;
    private boolean isCallSetUpRunnerMethod = false;

    /**
     * 具体设置参数
     *
     * @param jobName          MapReduce Job的名称
     * @param runnerClass      MapReduce类型
     * @param mapperClass      Mapper类型
     * @param reducerClass     Reducer类型
     * @param outputKeyClass   Reduce输出Key的类型
     * @param outputValueClass Reduce输出方式的类型
     */
    public void setupRunner(String jobName, Class<?> runnerClass, Class<? extends TableMapper<?, ?>>
            mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends
            WritableComparable<?>> outputKeyClass, Class<? extends Writable> outputValueClass) {
        this.setupRunner(jobName, runnerClass, mapperClass, reducerClass, outputKeyClass, outputValueClass,
                TransformerOutputFormat.class);
    }

    /**
     * 具体设置参数
     *
     * @param jobName           MapReduce Job的名称
     * @param mapReduceClass    MapReduce类型
     * @param mapperClass       Mapper类型
     * @param reducerClass      Reducer类型
     * @param outputKeyClass    Reduce输出Key的类型
     * @param outputValueClass  Reduce输出Value的类型
     * @param outputFormatClass Reduce输出方式的类型
     */
    public void setupRunner(String jobName, Class<?> mapReduceClass, Class<? extends TableMapper<?, ?>>
            mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends
            WritableComparable<?>> outputKeyClass, Class<? extends Writable> outputValueClass, Class<?
            extends OutputFormat<?, ?>> outputFormatClass) {
        this.setupRunner(jobName, mapReduceClass, mapperClass, reducerClass, outputKeyClass, outputValueClass,
                outputKeyClass, outputValueClass, outputFormatClass);
    }


    /**
     * 具体设置参数
     *
     * @param jobName             MapReduce Job的名称
     * @param mapReduceClass      MapReduce类型
     * @param mapperClass         Mapper类型
     * @param reducerClass        Reducer类型
     * @param mapOutputKeyClass   Map输出Key的类型
     * @param mapOutputValueClass Map输出Value的类型
     * @param outputKeyClass      Reduce输出Key的类型
     * @param outputValueClass    Reduce输出Value的类型
     */
    public void setupRunner(String jobName, Class<?> mapReduceClass, Class<? extends TableMapper<?, ?>>
            mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends
            WritableComparable<?>> mapOutputKeyClass, Class<? extends Writable> mapOutputValueClass,
                            Class<? extends WritableComparable<?>> outputKeyClass, Class<? extends
            Writable> outputValueClass) {
        this.setupRunner(jobName, mapperClass, mapperClass, reducerClass, mapOutputKeyClass,
                mapOutputValueClass, outputKeyClass, outputValueClass, TransformerOutputFormat.class);

    }

    /**
     * 设置参数
     *
     * @param jobName             MapReduce Job名称
     * @param mapReduceClass      MapReduce类型
     * @param mapperClass         Mapper类型
     * @param reducerClass        Reducer类型
     * @param mapOutputKeyClass   Map输出Key的类型
     * @param mapOutputValueClass Map输出Value的类型
     * @param outputKeyClass      Reduce输出Key的类型
     * @param outputValueClass    Reduce输出Value的类型
     * @param outputFormatClass   Reduce输出方式的类型
     */
    public void setupRunner(String jobName, Class<?> mapReduceClass, Class<? extends TableMapper<?, ?>>
            mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends
            WritableComparable<?>> mapOutputKeyClass, Class<? extends Writable> mapOutputValueClass,
                            Class<? extends WritableComparable<?>> outputKeyClass, Class<? extends
            Writable> outputValueClass, Class<? extends OutputFormat<?, ?>> outputFormatClass) {
        this.jobName = jobName;
        this.mapReduceClass = mapReduceClass;
        this.mapperClass = mapperClass;
        this.mapOutputKeyClass = mapOutputKeyClass;
        this.mapOutputValueClass = mapOutputValueClass;
        this.reducerClass = reducerClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.outputFormatClass = outputFormatClass;
        this.isCallSetUpRunnerMethod = true;
    }

    /**
     * 代码运行入口方法
     * @param args
     * @throws Exception
     */
    public void startRunner(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), this, args);
    }

    /**
     * 将指定的配置文件信息添加到conf中
     *
     * @param resourceFiles
     */
    protected void configure(String... resourceFiles) {
        //开始添加制定的配置文件
        if (resourceFiles != null && resourceFiles.length != 0) {
            for (String resourecFile : resourceFiles) {
                conf.addResource(resourecFile);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if (!isCallSetUpRunnerMethod) {
            throw new RuntimeException("必须先调用setupRunner方法进行参数设置");
        }
        Configuration conf = getConf();
        configure("output-collector.xml", "query-mapping.xml", "transformer-env.xml");

        //设置参数
        processArgs(conf, args);

        //创建MapReduce Job
        Job job = initJob(conf);

        Throwable error = null;

        //执行MapReduce Job之前的准备工作
        beforeRunJob(job);
        try {


            //执行MapReduce Job
            startTime = System.currentTimeMillis();
            int status = job.waitForCompletion(true) ? 0 : 1;
            return status;
        } catch (Exception e) {
            error = e;
            logger.error("执行" + jobName + " job出现异常", e);
            throw new RuntimeException(e);
        } finally {
            //执行MapReduce Job完成之后的工作
            afterRunJob(job, error);
        }
    }

    /**
     * 创建MapReduce Job
     *
     * @param conf
     * @return
     */
    protected Job initJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(this.getClass());

        TableMapReduceUtil.initTableMapperJob(initScan(job), mapperClass, mapOutputKeyClass,
                mapOutputValueClass, job, false);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setOutputFormatClass(outputFormatClass);
        return job;
    }

    /**
     * 执行MapReduce Job之前运行的方法
     *
     * @param job
     * @throws IOException
     */
    protected void beforeRunJob(Job job) throws IOException {
        //nothing
    }

    /**
     * 执行MapReduce Job之后运行的方法
     *
     * @param job MapReduce Job
     * @param error MapReduce Job运行过程中可能出现的异常
     * @throws IOException
     */
    protected void afterRunJob(Job job, Throwable error) throws IOException {
        //nothing
        try {
            long endTime = System.currentTimeMillis();
            logger.info("Job<" + jobName + ">是否执行成功" + error == null ? job.isSuccessful() : "false" +
                    "，开始时间：" +
                    startTime + "，结束时间：" + endTime + "，用时：" + (endTime - startTime) + " ms" + error == null
                    ? "" :
                    "异常信息为：" + error.getMessage());
        } catch (Throwable e) {
            //nothing
        }
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

        //设置HBase表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        Filter filter = fetchHbaseFilter();
        if (filter != null) {
            scan.setFilter(filter);
        }

        return Lists.newArrayList(scan);
    }

    /**
     * 获取HBase的过滤器
     *
     * @return 过滤器
     */
    protected Filter fetchHbaseFilter() {
        return null;
    }

    /**
     * 获取过滤给定列名的过滤器
     *
     * @param columns
     * @return
     */
    protected Filter getColumnFilter(String[] columns) {
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
     * @param conf HBase配置信息
     * @param args 参数
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
