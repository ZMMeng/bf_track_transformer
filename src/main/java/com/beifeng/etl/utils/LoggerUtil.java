package com.beifeng.etl.utils;


import com.beifeng.common.EventLogConstants;
import com.beifeng.utils.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * 处理日志数据的具体工作类
 * Created by 蒙卓明 on 2017/7/1.
 */
public class LoggerUtil {

    //打印日志的工具
    private static final Logger logger = Logger.getLogger(LoggerUtil.class);

    private static IpSeekerExt ipSeekerExt = new IpSeekerExt();

    /**
     * 处理日志数据logText
     *
     * @param logText 待处理的日志数据，以字符串形式传入
     * @return 返回处理结果Map集合，如果logText没有指定数据格式，则返回空集合
     */
    public static Map<String, String> handleLog(String logText) {
        Map<String, String> clientInfo = new HashMap<String, String>();
        //判断字符串是否非空
        if (StringUtils.isNotBlank(logText)) {
            String[] splits = logText.trim().split(EventLogConstants
                    .LOG_SEPARTIOR);
            if (splits.length == 4) {
                //日志格式为：ip地址^A服务器时间^Ahost主机^A请求参数
                //设置ip地址
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP,
                        splits[0].trim());
                //设置服务器时间
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                        String.valueOf(TimeUtil.parseNginxServerTime2Long
                                (splits[1].trim())));
                //设置
                int index = splits[3].indexOf("?");
                if (index == -1) {
                    //请求参数为空，表示数据格式异常，
                    clientInfo.clear();
                } else {
                    //请求参数不为空
                    //获取请求参数，即?后的内容
                    String requestBody = splits[3].substring(index + 1);
                    //处理请求参数
                    handleRequestBody(requestBody, clientInfo);
                    //处理userAgent
                    handleUserAgent(clientInfo);
                    //处理ip地址
                    handleIp(clientInfo);
                }
            }
        }
        return clientInfo;
    }

    /**
     * 获取IP地址对应的地域信息，并将结果存入一个Map集合中
     *
     * @param clientInfo
     */
    private static void handleIp(Map<String, String> clientInfo) {
        //确保clientInfo中保存有IP地址
        if (!clientInfo.containsKey(EventLogConstants.LOG_COLUMN_NAME_IP)) {
            return;
        }
        String ip = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_IP);
        IpSeekerExt.RegionInfo info = ipSeekerExt.analyticsIp(ip);
        if (info == null) {
            return;
        }
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, info
                .getCountry());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, info
                .getProvince());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, info.getCity());
    }

    /**
     * 处理浏览器的UserAgent信息，并将处理结果存入到Map集合中
     *
     * @param clientInfo
     */
    private static void handleUserAgent(Map<String, String> clientInfo) {
        //确保clientInfo中保存有UserAgent信息
        if (!clientInfo.containsKey(EventLogConstants
                .LOG_COLUMN_NAME_USER_AGENT)) {
            return;
        }
        UserAgentUtil.UserAgentInfo info = UserAgentUtil
                .analyticsUserAgent(clientInfo.get(EventLogConstants
                        .LOG_COLUMN_NAME_USER_AGENT));
        if (info == null) {
            return;
        }
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, info
                .getOsName());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, info
                .getOsVersion());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, info
                .getBrowserName());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
                info.getBrowserVersion());
    }

    /**
     * 处理请求参数字符串，并将处理结果存入到一个Map集合中
     *
     * @param requestBody 待处理的请求参数字符串
     * @param clientInfo  处理结果存入的Map集合
     */
    private static void handleRequestBody(String requestBody, Map<String,
            String> clientInfo) {
        //确保请求参数字符串非空
        if (StringUtils.isNotBlank(requestBody)) {
            //请求参数字符串为"aaa=bbb&ccc=ddd&....."
            String[] requestParams = requestBody.split("&");
            for (String param : requestParams) {
                if (StringUtils.isNotBlank(param)) {
                    int index = param.indexOf("=");
                    if (index == -1) {
                        logger.warn("无法进行解析参数" + param + "，请求参数为" +
                                requestBody);
                        continue;
                    }
                    String key = null, value = null;
                    try {
                        key = param.substring(0, index);
                        //传入的参数值是经过utf-8编码的，所以需要解码
                        value = URLDecoder.decode(param.substring(index + 1,
                                param.length()), "utf-8");
                    } catch (Exception e) {
                        logger.warn("解码操作出现异常", e);
                        continue;
                    }
                    //key和value都不为空的情况下才能填充
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank
                            (value)) {
                        clientInfo.put(key, value);
                    }
                }
            }
        }
    }
}
