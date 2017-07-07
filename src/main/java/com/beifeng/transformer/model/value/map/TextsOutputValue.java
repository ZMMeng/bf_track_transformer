package com.beifeng.transformer.model.value.map;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一系列的字符串输出类
 * Created by Administrator on 2017/7/7.
 */
public class TextsOutputValue extends BaseStatsValueWritable {

    //用户唯一标识符
    private String uuid;
    //会话ID
    private String sessionId;
    //KPI类型
    private KpiType kpi;

    public TextsOutputValue() {
    }

    public TextsOutputValue(String uuid, String sessionId) {
        this.uuid = uuid;
        this.sessionId = sessionId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setKpi(KpiType kpi){
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
        internalWrite(out, this.uuid);
        internalWrite(out, this.sessionId);
    }

    /**
     * 字段为空值时的序列化
     *
     * @param out
     * @param value
     * @throws IOException
     */
    private void internalWrite(DataOutput out, String value) throws IOException {
        if (StringUtils.isEmpty(value)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(value);
        }
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.uuid = internalReadFields(in);
        this.sessionId = internalReadFields(in);
    }

    /**
     * 当读取的字段为空时的反序列化方法
     * @param in
     * @return
     * @throws IOException
     */
    private String internalReadFields(DataInput in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
