package com.beifeng.etl.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;

import java.io.IOException;

/**
 * 解析浏览器UserAgent的工具类
 * 内部就是调用uasparser.jar文件
 * Created by Administrator on 2017/6/30.
 */
public class UserAgentUtil {

    private static UASparser uaSparser = null;

    /**
     * 初始化usSparser对象
     */
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析浏览器的userAgent字符串
     * @param userAgent userAgent字符串
     * @return 如果userAgent为空，则返回null，否则返回UserAgentInfo对象；如果解析失败，也返回null
     */
    public static UserAgentInfo analyticsUserAgent(String userAgent){
        if(userAgent == null || userAgent.trim().equals("")){
            return null;
        }
        UserAgentInfo result = null;
        cz.mallat.uasparser.UserAgentInfo info = null;
        try {
            //解析userAgent字符串
            info = uaSparser.parse(userAgent);
            result = new UserAgentInfo();
            //设置浏览器名称
            result.setBrowserName(info.getUaFamily());
            //设置浏览器版本号
            result.setBrowserVersion(info.getBrowserVersionInfo());
            //设置操作系统名称
            result.setOsName(info.getOsFamily());
            //设置操作系统版本号
            result.setOsVersion(info.getOsName());
        } catch (IOException e) {
            //出现异常，将返回值设置为null
            result = null;
        }
        return result;
    }

    /**
     * 内部解析后的浏览器信息model对象
     */
    public static class UserAgentInfo {

        //浏览器名称
        private String browserName;
        //浏览器版本号
        private String browserVersion;
        //操作系统名称
        private String osName;
        //操作系统版本号
        private String osVersion;

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}
