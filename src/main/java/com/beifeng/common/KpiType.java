package com.beifeng.common;

/**
 * 统计KPI的名称枚举类
 * Created by 蒙卓明 on 2017/7/2.
 */
public enum KpiType {

    NEW_INSTALL_USER("new_install_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user");

    public final String name;

    KpiType(String name) {
        this.name = name;
    }
}
