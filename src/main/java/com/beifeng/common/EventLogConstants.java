package com.beifeng.common;

/**
 * 定义收集到的用户数据参数的名称以及HBase表event_logs的结构信息
 * 注意，用户数据参数的名称就对应HBase表event_logs的列名
 * Created by 蒙卓明 on 2017/7/1.
 */
public class EventLogConstants {



    /**
     * 事件枚举类，指定事件的名称
     */
    public enum EventEnum {
        //lauch事件，表示第一次访问
        LAUNCH(1, "launch event", "e_1"),
        //页面浏览事件
        PAGEVIEW(2, "page view event", "e_pv"),
        //订单生成事件
        CHARGREQUEST(3, "charge request event", "e_crt"),
        //订单成功支付事件
        CHARGESUCCESS(4, "charge success event", "e_cs"),
        //订单退款事件
        CHARGEREFUND(5, "charge refund event", "e_cre"),
        //自定义事件
        EVENT(6, "event duration event", "ede");

        //id 唯一标识符
        private final int id;
        //事件名称
        private final String name;
        //事件别名，用于数据收集的简写
        private final String alias;

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getAlias() {
            return alias;
        }

        /**
         * 获取匹配别名的event枚举对象
         * @param alias 别名
         * @return 返回匹配别名的event枚举对象，无匹配则返回null
         */
        public static EventEnum valueOfAlias(String alias) {
            for (EventEnum event : values()) {
                if(event.alias.equals(alias)){
                    return event;
                }
            }
            return null;
        }
    }

    //HBase表名
    public static final String HBASE_TABLE_NAME = "event_logs";
    //HBase表名中的列簇名
    public static final String HBASE_COLUMN_FAMILY_NAME = "info";
    //日志分隔符
    public static final String LOG_SEPARTIOR = "\\^A";

    //HBase表列名
    //用户IP地址
    public static final String LOG_COLUMN_NAME_IP = "ip";
    //服务器时间
    public static final String LOG_COLUMN_NAME_SERVER_TIME = "s_time";
    //事件名称
    public static final String LOG_COLUMN_NAME_EVENT_NAME = "en";
    //数据收集端的版本信息
    public static final String LOG_COLUMN_NAME_VERSION = "ver";
    //用户唯一标识符
    public static final String LOG_COLUMN_NAME_UUID = "u_uid";
    //会员唯一标识符
    public static final String LOG_COLUMN_NAME_MEMBER_ID = "u_mid";
    //会话ID
    public static final String LOG_COLUMN_NAME_SESSION_ID = "u_sid";
    //客户端时间
    public static final String LOG_COLUMN_NAME_CLIENT_TIME = "c_time";
    //浏览器语言
    public static final String LOG_COLUMN_NAME_LANGUAGE = "l";
    //浏览器类型
    public static final String LOG_COLUMN_NAME_USER_AGENT = "b_iev";
    //浏览器分辨率
    public static final String LOG_COLUMN_NAME_RESOLUTIION = "b_rst";
    //当前链接
    public static final String LOG_COLUMN_NAME_CURRENT_URL = "p_url";
    //转入链接
    public static final String LOG_COLUMN_NAME_REFERENCE_URL = "p_ref";
    //页面标题
    public static final String LOG_COLUMN_NAME_TITLE = "tt";
    //订单ID
    public static final String LOG_COLUMN_NAME_ORDER_ID = "oid";
    //产品购买描述名称
    public static final String LOG_COLUMN_NAME_ORDER_NAME = "on";
    //产品价格
    public static final String LOG_COLUMN_NAME_CURRENCY_AMOUNT = "cua";
    //货币类型
    public static final String LOG_COLUMN_NAME_CURRENCY_TYPE = "cut";
    //支付方式
    public static final String LOG_COLUMN_NAME_PAYMENT_TYPE = "pt";
    //平台名称
    public static final String LOG_COLUMN_NAME_PLATFORM = "pl";
    //自定义事件名称
    public static final String LOG_COLUMN_NAME_CATEGORY = "ca";
    //自定义事件动作
    public static final String LOG_COLUMN_NAME_ACTION = "ac";
    //自定义事件相关属性名称的前缀
    public static final String LOG_COLUMN_NAME_KV_START = "kv_";
    //自定义事件持续时间
    public static final String LOG_COLUMN_NAME_DURATION = "du";
    //操作系统名称
    public static final String LOG_COLUMN_NAME_OS_NAME = "os";
    //操作系统版本号
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_v";
    //浏览器名称
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser";
    //浏览器版本号
    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_v";
    //ip对应的地域信息中的国家名
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";
    //ip对应的地域信息中的省份名
    public static final String LOG_COLUMN_NAME_PROVINCE = "province";
    //ip对应的地域信息中的城市名
    public static final String LOG_COLUMN_NAME_CITY = "city";


}
