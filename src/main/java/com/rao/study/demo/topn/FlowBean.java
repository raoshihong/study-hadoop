package com.rao.study.demo.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//实现WritableComparable
public class FlowBean implements WritableComparable<FlowBean> {

    private Long downFlow;
    private Long upFlow;
    private Long sumFlow;

    @Override
    public int compareTo(FlowBean o) {
        //内部根据总流量进行排序
        return o.sumFlow.compareTo(this.sumFlow);
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(downFlow);
        out.writeLong(upFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.downFlow = in.readLong();
        this.upFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public void set(long downFlow, long upFlow){
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        this.sumFlow = downFlow + upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return downFlow + "\t" + upFlow + "\t" + sumFlow;
    }
}
