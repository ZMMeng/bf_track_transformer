package com.beifeng.transformer.model.value.reduce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 蒙卓明 on 2017/7/8.
 */
public class InboundBounceReducerOutputValue extends BaseStatsValueWritable {

    //跳出外链数
    private int bounceSessions;
    //KPI
    private KpiType kpi;

    public InboundBounceReducerOutputValue(int bounceSessions) {
        this.bounceSessions = bounceSessions;
    }

    public int getBounceSessions() {
        return bounceSessions;
    }

    public void setBounceSessions(int bounceSessions) {
        this.bounceSessions = bounceSessions;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    /**
     * 获取当前value对应的KPI值
     *
     * @return
     */
    public KpiType getKpi() {
        return kpi;
    }

    /**
     * 自增
     */
    public void incrBounceSessions() {
        bounceSessions++;
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(bounceSessions);
        WritableUtils.writeEnum(out, kpi);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        bounceSessions = in.readInt();
        kpi = WritableUtils.readEnum(in, KpiType.class);
    }
}
