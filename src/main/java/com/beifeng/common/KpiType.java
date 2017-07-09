package com.beifeng.common;

/**
 * 统计KPI的名称枚举类
 * Created by 蒙卓明 on 2017/7/2.
 */
public enum KpiType {

    //统计新用户的KPI
    NEW_INSTALL_USER("new_install_user"),
    //统计浏览器维度的新用户KPI
    BROWSER_NEW_INSTALL_USER("browser_new_install_user"),
    //统计活跃用户KPI
    ACTIVE_USER("active_user"),
    //统计浏览器维度的活跃用户KPI
    BROWSER_ACTIVE_USER("browser_active_user"),
    //统计活跃会员KPI
    ACTIVE_MEMBER("active_member"),
    //统计浏览器维度的活跃会员KPI
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    //统计新增会员KPI
    NEW_MEMBER("new_member"),
    //统计浏览器维度的新增会员KPI
    BROWSER_NEW_MEMBER("browser_new_member"),
    //插入会员信息KPI
    INSERT_MEMBER_INFO("insert_member_info"),
    //统计会话KPI
    SESSIONS("sessions"),
    //统计浏览器维度会话KPI
    BROWSER_SESSIONS("browser_sessions"),
    //按小时统计活跃用户KPI
    HOURLY_ACTIVE_USER("hourly_active_user"),
    //按小时统计会话个数KPI
    HOURLY_SESSIONS("hourly_sessions"),
    //按小时统计会话长度KPI
    HOURLY_SESSIONS_LENGTH("hourly_sessions_length"),
    //统计浏览器维度的页面浏览量KPI
    PAGE_VIEW("page_view"),
    //统计地域信息的KPI
    LOCATION("location"),
    //统计外链分析的活跃用户和会话总数KPI
    INBOUND("inbound"),
    //统计外链分析的跳出会话个数的KPI
    INBOUND_BOUNCE("inbound_bounce"),
    ;

    public final String name;

    KpiType(String name) {
        this.name = name;
    }

    /**
     * 根据KPI名称获取KPI枚举对象
     *
     * @param name
     * @return
     */
    public static KpiType valueOfName(String name) {
        for (KpiType kpi : KpiType.values()) {
            if (kpi.name.equals(name)) {
                return kpi;
            }
        }
        throw new RuntimeException("指定的name不在KpiType枚举类的范围内：" + name);
    }
}
