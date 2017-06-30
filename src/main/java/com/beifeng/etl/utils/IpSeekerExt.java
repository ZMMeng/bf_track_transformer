package com.beifeng.etl.utils;

import com.beifeng.etl.utils.ip.IPSeeker;

/**
 * 定义具体的IP解析类，最终调用IPSeeker类
 * 最终返回 国家名 省份名 城市名
 * 国外 unknown unknown unknown
 * 国内无法解析IP 中国 unknown unknown
 * Created by Administrator on 2017/6/30.
 */
public class IpSeekerExt extends IPSeeker {

    /**
     * 解析IP地址
     *
     * @param ip IP地址字符串，例如：180.173.83.113
     * @return 返回该IP地址对应的地域信息对象
     */
    public RegionInfo analyticsIp(String ip) {
        RegionInfo result = new RegionInfo();
        if (ip == null || ip.trim().equals("")) {
            return result;
        }
        try {
            String info = super.getCountry(ip);
            if (info == null || info.trim().equals("")) {
                return result;
            }
            info = info.trim();

            int provinceIndex;
            int cityIndex;
            if (info.contains("省")) {
                result.setCountry("中国");
                //此时IP对应的省份是23个省份之一，包括台湾省，info的格式为xxx省(yyy市/州/地区)
                provinceIndex = info.indexOf("省");
                //设置省份名称 xxx省
                result.setProvince(info.substring(0, provinceIndex + 1));
                if (provinceIndex == info.length() - 1) {
                    //这种情况下，info只有省级行政区，没有地级行政区，格式为xxx省，比如说台湾省
                    return result;
                }
                //info由省级行政区和地级行政区组成，info格式为xxx省yyy市/州/地区
                if (info.contains("市")) {
                    //info格式为xxx省yyy市
                    cityIndex = info.indexOf("市");
                    result.setCity(info.substring(provinceIndex + 1,
                            cityIndex + 1));
                    return result;
                }
                if (info.contains("州")) {
                    //info格式为xxx省yyy州
                    cityIndex = info.indexOf("州");
                    result.setCity(info.substring(provinceIndex + 1,
                            cityIndex + 1));
                    return result;
                }
                if (info.contains("地区")) {
                    //info格式为xxx省yyy地区
                    cityIndex = info.indexOf("地区");
                    result.setCity(info.substring(provinceIndex + 1,
                            cityIndex + 2));
                    return result;
                }
            } else {
                //此时IP对应的省份是4个直辖市、5个自治区以及2个特别行政区，还有国外IP
                //4个直辖市和2个特别行政区
                if (info.contains("北京") || info.contains("上海") || info
                        .contains("天津") || info.contains("重庆") || info
                        .contains("香港") || info.contains("澳门")) {
                    //info格式为xxx市(直辖市)或者xxx(特别行政区)
                    result.setCountry("中国");
                    result.setProvince(info);
                    result.setCity(info);
                }
                //5个自治区
                if (info.contains("内蒙古")) {
                    //info格式为内蒙古yyy市/盟
                    result.setCountry("中国");
                    result.setProvince(info.substring(0, 3));
                    if (info.contains("市")) {
                        cityIndex = info.indexOf("市");
                        result.setCity(info.substring(4, cityIndex + 1));
                        return result;
                    }
                    if (info.contains("盟")) {
                        cityIndex = info.indexOf("盟");
                        result.setCity(info.substring(4, cityIndex + 1));
                        return result;
                    }
                }
                if (info.contains("广西") || info.contains("宁夏") || info
                        .contains("新疆") || info.contains("西藏")) {
                    //info格式为xxx(省级行政区)yyy市/州/地区
                    result.setCountry("中国");
                    result.setProvince(info.substring(0, 2));
                    if (info.contains("市")) {
                        //info格式为xxx(省级行政区)yyy市
                        cityIndex = info.indexOf("市");
                        result.setCity(info.substring(3, cityIndex + 1));
                        return result;
                    }
                    if (info.contains("州")) {
                        //info格式为xxx(省级行政区)yyy州
                        cityIndex = info.indexOf("州");
                        result.setCity(info.substring(3, cityIndex + 1));
                        return result;
                    }
                    if (info.contains("地区")) {
                        //info格式为xxx(省级行政区)yyy地区
                        cityIndex = info.indexOf("地区");
                        result.setCity(info.substring(3, cityIndex + 2));
                        return result;
                    }
                }

                return result;
            }
        } catch (Exception e) {
            return result;
        }
        return result;
    }

    /**
     * IP地域相关的一个Model类
     */
    public static class RegionInfo {
        //默认值
        private static final String DEFAULT_VALUE = "unknown";
        //国家名称
        private String country = DEFAULT_VALUE;
        //省份名称
        private String province = DEFAULT_VALUE;
        //城市名称
        private String city = DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }
}
