package com.beifeng.transformer.service.rpc.client;

import com.beifeng.transformer.model.dimension.basic.BaseDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.server.DimensionConverterImpl;
import com.beifeng.transformer.service.rpc.server.DimensionConverterServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * 操作DimesionConverter相关服务的客户端工具类
 * Created by 蒙卓明 on 2017/7/10.
 */
public class DimensionConverterClient {

    /**
     * 创建连接对象
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static IDimensionConverter createDimensionConverter(Configuration conf) throws IOException {

        //创建操作
        String[] cf = fetchDimensionConverterConfiguration(conf);
        //获取端口号和IP地址
        int port = Integer.valueOf(cf[1]);
        String address = cf[0];
        //从代理类创建converter对象
        IDimensionConverter converter = new InnerDimensionConverterProxy(conf, address, port);
        return converter;
    }

    /**
     * 关闭客户端连接
     *
     * @param proxy
     */
    public static void stopDimensionConverterProxy(IDimensionConverter proxy) {
        if (proxy != null) {
            InnerDimensionConverterProxy innerProxy = (InnerDimensionConverterProxy) proxy;
            RPC.stopProxy(innerProxy.proxy);
        }
    }

    /**
     * 读取配置信息
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String[] fetchDimensionConverterConfiguration(Configuration conf) throws IOException {
        FileSystem fs = null;
        BufferedReader br = null;
        try{
            fs = FileSystem.get(conf);
            br = new BufferedReader(new InputStreamReader(fs.open(new Path(DimensionConverterServer
                    .CONFIG_SAVE_PATH))));
            String[] result = new String[2];
            //IP地址
            result[0] = br.readLine().trim();
            //端口号
            result[1] = br.readLine().trim();
            return result;
        } finally {
            if(br != null){
                try {
                    br.close();
                }catch (Exception e){
                    //nothing
                }
            }
            /*if(fs != null){
                try{
                    fs.close();
                }catch (Exception e){
                    //nothing
                }
            }*/
        }
    }

    /**
     * 内部代理类
     */
    public static class InnerDimensionConverterProxy implements IDimensionConverter {

        private IDimensionConverter proxy = null;

        private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {

            /**
             * 超过1000个不保存
             * @param eldest
             * @return
             */
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return this.size() > 1000;
            }
        };

        //private static final long serialVersionUID = -731083744087467205L;

        public InnerDimensionConverterProxy(Configuration conf, String address, int port) throws IOException {
            this.proxy = RPC.getProxy(IDimensionConverter.class, IDimensionConverter.versionID, new
                    InetSocketAddress(address, port), conf);
        }

        /**
         * 根据dimension的value值获取id
         *
         * @param dimension
         * @return 如果数据库中有直接返回，如果没有，则在数据库中插入后返回新的id
         * @throws IOException
         */
        public int getDimensionIdByValue(BaseDimension dimension) throws IOException {
            String key = DimensionConverterImpl.buildCacheKey(dimension);
            Integer value = cache.get(key);
            if (value == null) {
                //通过proxy获取数据
                value = proxy.getDimensionIdByValue(dimension);
                cache.put(key, value);
            }
            return value;
        }


        public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
            return this.proxy.getProtocolVersion(protocol, clientVersion);
        }


        public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int
                clientMethodsHash) throws IOException {
            return this.proxy.getProtocolSignature(protocol, clientVersion, clientMethodsHash);
        }
    }
}
