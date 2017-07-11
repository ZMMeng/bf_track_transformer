package com.beifeng.transformer.hive.orders;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dimension.basic.CurrencyTypeDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 订单支付货币类型Dimension操作UDF
 * Created by Administrator on 2017/7/11.
 */
public class CurrencyTypeDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public CurrencyTypeDimensionUDF() {
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
     * 根据给定的货币类型名称，获取相应的ID
     *
     * @param currencyName 定的货币类型名称
     * @return 相应的ID
     */
    public IntWritable evaluate(Text currencyName) {
        //判断currencyName是否为空，为空则赋予默认值
        if (currencyName == null || StringUtils.isBlank(currencyName.toString().trim())) {
            currencyName = new Text(GlobalConstants.DEFAULT_VALUE);
        }

        CurrencyTypeDimension dimension = new CurrencyTypeDimension(currencyName.toString());
        try {
            int id = converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
