package com.beifeng.transformer.hive.viewdepth;

import com.beifeng.transformer.model.dimension.basic.PlatformDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 操作平台维度相关的UDF
 * Created by Administrator on 2017/7/10.
 */
public class PlatformDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public PlatformDimensionUDF() {
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
     * 根据给定的平台名称返回对应的id
     *
     * @param platformName
     * @return
     */
    public IntWritable evaluate(Text platformName) {
        PlatformDimension dimension = new PlatformDimension(platformName.toString());
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
