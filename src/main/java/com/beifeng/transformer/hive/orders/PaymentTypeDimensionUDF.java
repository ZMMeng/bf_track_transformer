package com.beifeng.transformer.hive.orders;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dimension.basic.PaymentTypeDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 订单支付类型Dimension操作UDF
 * Created by Administrator on 2017/7/11.
 */
public class PaymentTypeDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public PaymentTypeDimensionUDF() {
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
     * 根据给定的支付类型名称，获取相应的ID
     *
     * @param paymentType 给定的支付类型名称
     * @return 相应的ID
     */
    public IntWritable evaluate(Text paymentType) {
        //判断paymentType是否为空，为空则赋予默认值
        if (paymentType == null || StringUtils.isBlank(paymentType.toString().trim())) {
            paymentType = new Text(GlobalConstants.DEFAULT_VALUE);
        }

        PaymentTypeDimension dimension = new PaymentTypeDimension(paymentType.toString());
        try {
            int id = converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取ID异常");
        }
    }
}
