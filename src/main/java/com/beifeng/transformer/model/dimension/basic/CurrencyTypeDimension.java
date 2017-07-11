package com.beifeng.transformer.model.dimension.basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 货币类型维度类
 * Created by Administrator on 2017/7/11.
 */
public class CurrencyTypeDimension extends BaseDimension {

    //ID
    private int id;
    //货币名称
    private String currencyName;

    public CurrencyTypeDimension() {
        super();
    }

    public CurrencyTypeDimension(String currencyName) {
        this.currencyName = currencyName;
    }

    public CurrencyTypeDimension(int id, String currencyName) {
        this.id = id;
        this.currencyName = currencyName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrencyName() {
        return currencyName;
    }

    public void setCurrencyName(String currencyName) {
        this.currencyName = currencyName;
    }

    /**
     * 实现比较
     *
     * @param o
     * @return
     */
    public int compareTo(BaseDimension o) {
        //判断是否是同一个对象
        if (this == o) {
            //是同一对象则直接返回相等
            return 0;
        }
        //不是同一对象则将o强转
        CurrencyTypeDimension other = (CurrencyTypeDimension) o;
        //首先比较id
        int tmp = Integer.valueOf(id).compareTo(other.id);
        //判断id比较结果是否为零
        if (tmp != 0) {
            //id比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //id比较结果为零，直接返回currencyName的比较结果
        return currencyName.compareTo(other.currencyName);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(currencyName);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        currencyName = in.readUTF();
    }
}
