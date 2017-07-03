package com.beifeng.transformer.model.value.reduce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 蒙卓明 on 2017/7/2.
 */
public class MapWritableValue extends BaseStatsValueWritable{

    private MapWritable value = new MapWritable();
    private KpiType kpi;

    public MapWritableValue() {
        super();
    }

    public MapWritableValue(MapWritable value, KpiType kpi) {
        this.value = value;
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public KpiType getKpi() {
        return kpi;
    }

    /**
     * 序列化，注意枚举类型的序列化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        value.write(out);
        WritableUtils.writeEnum(out, kpi);
    }

    /**
     * 反序列化，注意枚举类型的反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        value.readFields(in);
        kpi = WritableUtils.readEnum(in, KpiType.class);
    }
}
