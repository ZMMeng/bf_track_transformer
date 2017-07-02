package com.beifeng.transformer.model.value;

import com.beifeng.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * 自定义顶级的输出Value父类
 * Created by 蒙卓明 on 2017/7/2.
 */
public abstract class BaseStatsValueWritable implements Writable {

    /**
     * 获取当前value对应的KPI值
     *
     * @return
     */
    public abstract KpiType getKpi();
}
