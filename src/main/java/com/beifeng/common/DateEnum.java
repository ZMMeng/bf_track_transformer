package com.beifeng.common;

/**
 * 日期类型枚举类
 * Created by 蒙卓明 on 2017/7/2.
 */
public enum DateEnum {
    //年
    YEAR("yaer"),
    //季度
    SEASON("season"),
    //月
    MONTH("month"),
    //周
    WEEK("week"),
    //天
    DAY("day"),
    //小时
    HOUR("hour");

    public final String name;

    DateEnum(String name) {
        this.name = name;
    }

    /**
     * 根据属性name的值获取对应的type对象
     *
     * @param name
     * @return
     */
    public static DateEnum valueOfName(String name) {
        for (DateEnum type : DateEnum.values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }
        return null;
    }
}
