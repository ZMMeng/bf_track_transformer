package com.beifeng.transformer.model.dimension.basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 外链信息维度对象，注意这一维度，不需要向数据库中插入新的数据，因此，除了id之外的所有属性，均作为描述信息
 * Created by 蒙卓明 on 2017/7/7.
 */
public class InboundDimension extends BaseDimension {

    //此外链id
    private int id;
    //父级外链id
    private int parentId;
    //外链名称
    private String name;
    //外链URL
    private String url;
    //类型
    private int type;

    public InboundDimension() {
    }

    public InboundDimension(int id, int parentId, String name, String url, int type) {
        this.id = id;
        this.parentId = parentId;
        this.name = name;
        this.url = url;
        this.type = type;
    }

    public InboundDimension(InboundDimension inbound){
        super();
        this.id = inbound.id;
        this.parentId = inbound.parentId;
        this.name = inbound.name;
        this.url = inbound.url;
        this.type = inbound.type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getParentId() {
        return parentId;
    }

    public void setParentId(int parentId) {
        this.parentId = parentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    /**
     * 实现比较
     * @param o
     * @return
     */
    public int compareTo(BaseDimension o) {
        //只需要比较id即可
        return Integer.valueOf(id).compareTo(Integer.valueOf(((InboundDimension)o).id));
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
    }
}
