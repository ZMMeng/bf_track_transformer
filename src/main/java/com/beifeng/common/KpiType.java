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
    BROWSER_ACTIVE_MEMBER("browser_active_member");

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
