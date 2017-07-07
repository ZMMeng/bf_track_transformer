package com.beifeng.transformer.model.dimension.basic;

import com.beifeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 地域维度信息对象，对应dimension_location表
 * Created by Administrator on 2017/7/7.
 */
public class LocationDimension extends BaseDimension {

    //id
    private int id;
    //国家
    private String country;
    //省份
    private String province;
    //城市
    private String city;

    public LocationDimension() {
        super();
        this.clean();
    }

    public LocationDimension(int id, String country, String province, String city) {
        this.id = id;
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    /**
     * 清空LocationDimension对象
     */
    public void clean(){
        this.id = 0;
        this.country = "";
        this.province = "";
        this.city = "";
    }

    /**
     * 创建LocationDimension对象
     * @param country
     * @param province
     * @param city
     * @return
     */
    public static LocationDimension newInstance(String country, String province, String city){
        LocationDimension location = new LocationDimension();
        location.country = country;
        location.province = province;
        location.city = city;
        return location;
    }

    /**
     * 构造多地域维度对象
     * @param country
     * @param province
     * @param city
     * @return
     */
    public static List<LocationDimension> buildList(String country, String province, String city){
        List<LocationDimension> list = new ArrayList<LocationDimension>();
        //判断country是否非空或者为默认值
        if(StringUtils.isBlank(country) || GlobalConstants.DEFAULT_VALUE.equals(country)){
            //国家名为空，将三个字段都设置为默认值
            country = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        //判断province是否为空或者为默认值
        if(StringUtils.isBlank(province) || GlobalConstants.DEFAULT_VALUE.equals(province)){
            //省份名为空，将省份名和城市名设置为默认值
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        //判断city是否为空
        if(StringUtils.isBlank(city)){
            //城市名为空，将城市为设置为默认值
            city = GlobalConstants.DEFAULT_VALUE;
        }

        //国家级别
        list.add(newInstance(country, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL));
        //省份级别
        list.add(newInstance(country, province, GlobalConstants.VALUE_OF_ALL));
        //城市级别
        list.add(newInstance(country, province, city));
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocationDimension that = (LocationDimension) o;

        if (id != that.id) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        if (province != null ? !province.equals(that.province) : that.province != null) return false;
        return city != null ? city.equals(that.city) : that.city == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (province != null ? province.hashCode() : 0);
        result = 31 * result + (city != null ? city.hashCode() : 0);
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
        LocationDimension other = (LocationDimension) o;

        //首先比较id
        int tmp = Integer.valueOf(id).compareTo(other.id);
        //判断id比较结果是否为零
        if (tmp != 0) {
            //id比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //id比较结果为零，比较country
        tmp = country.compareTo(other.country);
        //判断country比较结果是否为零
        if(tmp != 0){
            //country比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //country比较结果为零，比较province
        tmp = province.compareTo(other.province);
        //判断country比较结果是否为零
        if(tmp != 0){
            //province比较较结果不为零，直接返回比较结果
            return tmp;
        }
        //province比较结果为零，直接返回city比较结果
        return city.compareTo(other.city);
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(country);
        out.writeUTF(province);
        out.writeUTF(city);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        country = in.readUTF();
        province = in.readUTF();
        city = in.readUTF();
    }
}