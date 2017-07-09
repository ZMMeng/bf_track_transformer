package com.beifeng.transformer.model.value.reduce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 统计外链分析时，Reduce输出的value类
 * Created by 蒙卓明 on 2017/7/8.
 */
public class InboundReducerOutputValue extends BaseStatsValueWritable {

    //会话个数
    private int sessions;
    //活跃会员数
    private int activeUsers;
    //KPI
    private KpiType kpi;

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getActiveUsers() {
        return activeUsers;
    }

    public void setActiveUsers(int activeUsers) {
        this.activeUsers = activeUsers;
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
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(sessions);
        out.writeInt(activeUsers);
        WritableUtils.writeEnum(out, kpi);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        sessions = in.readInt();
        activeUsers = in.readInt();
        WritableUtils.readEnum(in, KpiType.class);
    }
}
