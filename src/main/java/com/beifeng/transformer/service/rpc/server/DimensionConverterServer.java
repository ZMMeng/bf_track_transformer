package com.beifeng.transformer.service.rpc.server;

import com.beifeng.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * IDimensionConverter服务接口的启动类
 * Created by 蒙卓明 on 2017/7/9.
 */
public class DimensionConverterServer {

    public static final String CONFIG_SAVE_PATH = "/beifeng/transformer/rpc/config";
    //日志打印对象
    private static final Logger logger = Logger.getLogger(DimensionConverterServer.class);
    //标识是否启动
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    //服务对象
    private Server server = null;
    //配置对象
    private Configuration conf;

    public DimensionConverterServer(Configuration conf) {
        this.conf = conf;
        //添加一个钩子，进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                try {
                    DimensionConverterServer.this.removeListenerAddress();
                } catch (Exception e) {
                    //nothing
                } finally {
                    try{
                        DimensionConverterServer.this.stopServer();
                    }catch (Exception e){
                        //nothing
                    }

                }
            }
        }));
    }

    /**
     * 关闭服务
     */
    public void stopServer() throws IOException {
        logger.info("关闭服务开始");
        try {
            removeListenerAddress();
        }finally {
            try{
                if (server != null) {
                    server.stop();
                    server = null;
                }
            }catch (Exception e){
                //nothing
            }
        }
        logger.info("关闭服务结束");
    }

    /**
     * 启动服务
     */
    public void startServer() {
        logger.info("开始启动服务");
        synchronized (this) {
            if (isRunning.get()) {
                //启动完成
                return;
            }

            try {
                //创建对象
                IDimensionConverter converter = new DimensionConverterImpl();
                //创建服务
                if (server == null) {
                    server = new RPC.Builder(conf).setInstance(converter).setProtocol(IDimensionConverter
                            .class).setVerbose(true).build();
                }
                //获取IP和端口号
                int port = server.getPort();
                String address = InetAddress.getLocalHost().getHostAddress();

                saveListenerAddress(address, port);

                //启动
                server.start();
                //标识成功
                isRunning.set(true);
                //
                logger.info("启动服务成功，监听ip地址：" + address + "，端口：" + port);
            } catch (Exception e) {
                isRunning.set(false);
                logger.error("启动服务发生异常", e);
                //关闭可能异常创建的服务
                try {
                    stopServer();
                } catch (Exception er) {
                    //nothing
                }
                throw new RuntimeException("启动服务发生异常", e);
            }
        }
    }

    /**
     * 保存监听信息
     *
     * @param address
     * @param port
     * @throws IOException
     */
    private void saveListenerAddress(String address, int port) throws IOException {
        //删除已经存在的监听信息
        removeListenerAddress();

        //进行数据输出操作
        FileSystem fs = null;
        BufferedWriter bw = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(CONFIG_SAVE_PATH);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
            bw.write(address);
            bw.newLine();
            bw.write(String.valueOf(port));
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    //nothing
                }
            }
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    //nothing
                }
            }
        }
    }

    /**
     * 删除监听信息
     *
     * @throws IOException
     */
    private void removeListenerAddress() throws IOException {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(CONFIG_SAVE_PATH);
            if (fs.exists(path)) {
                //存在则删除
                fs.delete(path, true);
            }
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    //nothing
                }
            }
        }
    }
}
