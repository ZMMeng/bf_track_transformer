package com.beifeng.transformer.service.rpc;

import com.beifeng.transformer.service.rpc.server.DimensionConverterServer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by Administrator on 2017/7/10.
 */
public class TestRpc {

    public static void main(String[] args){
        Configuration conf = new Configuration();
        DimensionConverterServer dcs = new DimensionConverterServer(conf);
        dcs.startServer();
    }
}
