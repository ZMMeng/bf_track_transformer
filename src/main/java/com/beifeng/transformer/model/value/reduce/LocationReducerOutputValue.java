package com.beifeng.transformer.model.value.reduce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义统计地域信息的MapReduce Reducer输出value类
 * Created by Administrator on 2017/7/7.
 */
public class LocationReducerOutputValue extends BaseStatsValueWritable {

    //活跃用户数
    private int activeUsers;
    //会话个数
    private int sessions;
    //跳出会话个数
    private int bounceSessions;
    //KPI
    private KpiType kpi;

    public LocationReducerOutputValue() {
    }

    public int getActiveUsers() {
        return activeUsers;
    }

    public void setActiveUsers(int activeUsers) {
        this.activeUsers = activeUsers;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
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
     * 序列化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(activeUsers);
        out.writeInt(sessions);
        out.writeInt(bounceSessions);
        WritableUtils.writeEnum(out, KpiType.LOCATION);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        activeUsers = in.readInt();
        sessions = in.readInt();
        bounceSessions = in.readInt();
        kpi = WritableUtils.readEnum(in, KpiType.class);
    }
}
