package com.beifeng.transformer.util;

/**
 * 用于计算会话长度的类
 * Created by Administrator on 2017/7/5.
 */
public class TimeChain {

    //存储长度
    private static final int CHAIN_SIZE = 2;
    //存储的数据长度
    private long[] times;
    //当前下标
    private int index;
    //当前数据个数
    private int size;
    //
    private int tmpTime;

    public TimeChain(long time){
        this.times = new long[CHAIN_SIZE];
        this.times[0] = time;
        this.index = 0;
        this.size = 1;
        this.tmpTime = 0;
    }

    public int getTmpTime() {
        return tmpTime;
    }

    public void setTmpTime(int tmpTime) {
        this.tmpTime = tmpTime;
    }

    /**
     * 向times数组添加元素，只保存最小时间和最大时间
     * @param time
     */
    public void addTime(long time){
        //先判断此时times长度
        if(size == 1){
            //此时times数组中只有一个元素
            //获取times数组当前的唯一值
            long temp = this.times[this.index];
            //两者相比较
            if(temp > time){
                this.times[0] = time;
                this.times[1] = temp;
            }else{
                this.times[1] = time;
            }
            this.index++;
            this.size++;
        }else{
            long first = this.times[0];
            long second = this.times[1];
            if(time < first){
                this.times[0] = time;
            }else if(time > second){
                this.times[1] = time;
            }
        }
    }

    /**
     * 获取最小时间戳
     * @return
     */
    public long getMinTime(){
        return this.times[0];
    }

    /**
     * 获取最大时间戳
     * @return
     */
    public long getMaxTime(){
        return this.times[this.size - 1];
    }

    /**
     * 获取时间间隔(毫秒数)
     * @return
     */
    public long getTimeOfMills(){
        return getMaxTime() - getMinTime();
    }

    /**
     * 获取时间间隔(秒数)
     * @return
     */
    public int getTimeOfSecond(){
        return (int)(getTimeOfMills() / 1000);
    }

}
