package com.beifeng.transformer.model.dim.base;

import com.beifeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 平台维度类
 * Created by 蒙卓明 on 2017/7/2.
 */
public class PlatformDimension extends BaseDimension {

    //id
    private int id;
    //平台名称
    private String platformName;

    public PlatformDimension() {
        super();
    }

    public PlatformDimension(String platformName) {
        super();
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this.id = id;
        this.platformName = platformName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    /**
     * 构建含多个平台维度信息对象的集合
     * @param platformName 平台名称
     * @return 含多个平台维度信息对象的集合
     */
    public static List<PlatformDimension> buildList(String platformName) {
        //判断platformName是否为空
        if(StringUtils.isBlank(platformName)){
            //如果platformName为空，将其赋值为默认值unknown
            platformName = GlobalConstants.DEFAULT_VALUE;
        }

        List<PlatformDimension> list = new ArrayList<PlatformDimension>();
        //list.add(new PlatformDimension(GlobalConstants.VALUE_OF_ALL));
        list.add(new PlatformDimension(platformName));
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlatformDimension)) return false;

        PlatformDimension that = (PlatformDimension) o;

        if (id != that.id) return false;
        return platformName != null ? platformName.equals(that.platformName)
                : that.platformName == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (platformName != null ? platformName.hashCode() : 0);
        return result;
    }

    /**
     * 实现比较
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
        PlatformDimension other = (PlatformDimension) o;

        //首先比较id
        int tmp = Integer.valueOf(id).compareTo(other.id);
        //判断id比较结果是否为零
        if (tmp != 0) {
            //id比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //id比较结果为零，直接返回platformName的比较结果
        return platformName.compareTo(other.platformName);
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(platformName);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        platformName = in.readUTF();
    }
}