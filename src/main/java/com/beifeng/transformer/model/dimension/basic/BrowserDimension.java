package com.beifeng.transformer.model.dimension.basic;

import com.beifeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 浏览器维度
 * Created by 蒙卓明 on 2017/7/2.
 */
public class BrowserDimension extends BaseDimension {

    //id
    private int id;
    //浏览器名称
    private String browserName;
    //浏览器版本号
    private String browserVersion;

    public BrowserDimension() {
        super();
    }

    public BrowserDimension(String browserName, String browserVersion) {
        super();
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    /**
     * 根据现有的浏览器名称和浏览器版本号创建实例
     *
     * @param browserName
     * @param browserVersion
     * @return
     */
    public static BrowserDimension newInstance(String browserName, String
            browserVersion) {
        return new BrowserDimension(browserName, browserVersion);
    }

    /**
     * 构建含多个浏览器维度信息对象的集合
     *
     * @param browserName 浏览器名称
     * @param browserVersion 浏览器版本号
     * @return 含多个浏览器维度信息对象的集合
     */
    public static List<BrowserDimension> buildList(String browserName, String
            browserVersion) {
        List<BrowserDimension> list = new ArrayList<BrowserDimension>();
        //判断浏览器名称是否为空
        if (StringUtils.isBlank(browserName)) {
            //浏览器名称为空，则将browserName和browserVersion设置为默认值unknown
            browserName = GlobalConstants.DEFAULT_VALUE;
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        //判断浏览器版本号是否为空，针对浏览器名称不为空，浏览器版本号为空的情况
        if (StringUtils.isEmpty(browserVersion)) {
            //浏览器版本号不为空，将browserVersion设置为默认值unknown
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        //添加两个浏览器维度信息对象
        /*list.add(BrowserDimension.newInstance(GlobalConstants.VALUE_OF_ALL,
                GlobalConstants.VALUE_OF_ALL));*/
        list.add(BrowserDimension.newInstance(browserName, GlobalConstants
                .VALUE_OF_ALL));
        list.add(BrowserDimension.newInstance(browserName, browserVersion));
        return list;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    /**
     * 清空浏览器的维度信息
     */
    public void clean(){
        id = 0;
        browserName = "";
        browserVersion = "";
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
        BrowserDimension other = (BrowserDimension) o;

        //首先比较id
        int tmp = Integer.valueOf(id).compareTo(other.id);
        //判断id比较结果是否为零
        if (tmp != 0) {
            //id比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //id比较结果为零，比较BrowserName
        tmp = browserName.compareTo(other.browserName);
        //判断browserName比较结果是否为零‘
        if(tmp != 0){
            //browserName比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //browserName比较结果为零，直接返回browserVersion比较结果
        return browserVersion.compareTo(other.browserVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrowserDimension)) return false;

        BrowserDimension that = (BrowserDimension) o;

        if (id != that.id) return false;
        if (browserName != null ? !browserName.equals(that.browserName) :
                that.browserName != null)
            return false;
        return browserVersion != null ? browserVersion.equals(that
                .browserVersion) : that.browserVersion == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (browserName != null ? browserName.hashCode()
                : 0);
        result = 31 * result + (browserVersion != null ? browserVersion.hashCode() : 0);
        return result;
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(browserName);
        out.writeUTF(browserVersion);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        browserName = in.readUTF();
        browserVersion = in.readUTF();
    }
}
