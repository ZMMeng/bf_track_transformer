package com.beifeng.transformer.hive.event;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dimension.basic.EventDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 计算event相关数据的UDF
 * Created by Administrator on 2017/7/11.
 */
public class EventDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public EventDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException("创建converter异常");
        }

        //添加一个钩子进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                try {
                    DimensionConverterClient.stopDimensionConverterProxy(converter);
                } catch (Exception e) {
                    //nothing
                }

            }
        }));
    }

    /**
     * 根据给定的种类名称和动作名称获取相应事件的ID
     *
     * @param category 种类名称
     * @param action   动作名称
     * @return 相应事件的ID
     */
    public IntWritable evaluate(Text category, Text action) {
        //判断category和action是否为空，为空则赋默认值
        if (category == null || StringUtils.isBlank(category.toString().trim())) {
            category = new Text(GlobalConstants.DEFAULT_VALUE);
        }
        if (action == null || StringUtils.isBlank(action.toString().trim())) {
            action = new Text(GlobalConstants.DEFAULT_VALUE);
        }
        EventDimension dimension = new EventDimension(category.toString(), action.toString());
        try {
            int id = converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }

    public static void main(String[] args) {
        System.out.println(new EventDimensionUDF().evaluate(new Text("event的category名称"), new Text
                ("event的action名称")).get());
    }
}
