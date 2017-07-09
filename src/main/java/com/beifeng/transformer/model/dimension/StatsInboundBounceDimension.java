package com.beifeng.transformer.model.dimension;

import com.beifeng.transformer.model.dimension.basic.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 蒙卓明 on 2017/7/8.
 */
public class StatsInboundBounceDimension extends StatsDimension {

    //公用维度信息
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    //会话ID
    private String sessionId;
    //服务器时间
    private long serverTime;

    public StatsInboundBounceDimension() {
    }

    public StatsInboundBounceDimension(StatsCommonDimension statsCommon, String sessionId, long serverTime) {
        this.statsCommon = statsCommon;
        this.sessionId = sessionId;
        this.serverTime = serverTime;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getServerTime() {
        return serverTime;
    }

    public void setServerTime(long serverTime) {
        this.serverTime = serverTime;
    }

    /**
     * 复制对象
     *
     * @param dimension
     * @return
     */
    public static StatsInboundBounceDimension clone(StatsInboundBounceDimension dimension) {
        return new StatsInboundBounceDimension(dimension.getStatsCommon(), dimension.getSessionId(),
                dimension.getServerTime());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsInboundBounceDimension)) return false;

        StatsInboundBounceDimension that = (StatsInboundBounceDimension) o;

        if (serverTime != that.serverTime) return false;
        if (statsCommon != null ? !statsCommon.equals(that.statsCommon) : that.statsCommon != null)
            return false;
        return sessionId != null ? sessionId.equals(that.sessionId) : that.sessionId == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommon != null ? statsCommon.hashCode() : 0;
        result = 31 * result + (sessionId != null ? sessionId.hashCode() : 0);
        result = 31 * result + (int) (serverTime ^ (serverTime >>> 32));
        return result;
    }

    /**
     * 实现比较
     * @param o
     * @return
     */
    public int compareTo(BaseDimension o) {
        //判断是否是同一对象
        if (this == o) {
            //同一对象，直接返回0
            return 0;
        }
        //强转
        StatsInboundBounceDimension other = (StatsInboundBounceDimension) o;
        //首先比较statsCommon
        int tmp = statsCommon.compareTo(other.statsCommon);
        //判断statsCommon比较结果
        if(tmp != 0){
            //statsCommon比较结果不为零，直接返回statsCommon比较结果
            return tmp;
        }
        //statsCommon比较结果为零，比较sessionId
        tmp = sessionId.compareTo(other.sessionId);
        //判断sessionId比较结果
        if(tmp != 0){
            //sessionId比较结果不为零，直接返回sessionId比较结果
            return tmp;
        }
        //sessionId比较结果为零，直接返回serverTime的比较结果
        return Long.valueOf(serverTime).compareTo(other.serverTime);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        statsCommon.write(out);
        out.writeUTF(sessionId);
        out.writeLong(serverTime);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        statsCommon.readFields(in);
        sessionId = in.readUTF();
        serverTime = in.readLong();
    }
}
