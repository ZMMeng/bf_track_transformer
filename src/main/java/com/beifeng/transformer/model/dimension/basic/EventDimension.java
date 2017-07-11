package com.beifeng.transformer.model.dimension.basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2017/7/11.
 */
public class EventDimension extends BaseDimension {

    //ID
    private int id;
    //种类名称
    private String category;
    //动作名称
    private String action;

    public EventDimension() {
        super();
    }

    public EventDimension(String category, String action) {
        super();
        this.category = category;
        this.action = action;
    }

    public EventDimension(int id, String category, String action) {
        super();
        this.id = id;
        this.category = category;
        this.action = action;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
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
        EventDimension other = (EventDimension) o;

        //首先比较id
        int tmp = Integer.valueOf(id).compareTo(other.id);
        //判断id比较结果是否为零
        if (tmp != 0) {
            //id比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //id比较结果为零，比较category
        tmp = category.compareTo(other.category);
        //判断category比较结果是否为零‘
        if (tmp != 0) {
            //category比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //category比较结果为零，直接返回action比较结果
        return action.compareTo(other.action);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(category);
        out.writeUTF(action);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        category = in.readUTF();
        action = in.readUTF();
    }
}
