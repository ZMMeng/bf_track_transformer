package com.beifeng.transformer.hive.viewdepth;

import com.beifeng.common.DateEnum;
import com.beifeng.transformer.model.dimension.basic.DateDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import com.beifeng.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 日期维度相关的UDF
 * Created by Administrator on 2017/7/10.
 */
public class DateDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public DateDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException("创建converter异常");
        }

        //添加一个钩子进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                try{
                    DimensionConverterClient.stopDimensionConverterProxy(converter);
                }catch (Exception e){
                    //nothing
                }

            }
        }));
    }

    /**
     * 根据给定的日期（格式为:yyyy-MM-dd）至返回id
     *
     * @param day
     * @return
     */
    public IntWritable evaluate(Text day) {
        DateDimension dimension = DateDimension.buildDate(TimeUtil.parseString2Long(day.toString()), DateEnum.DAY);
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
