package com.rao.study.comparable;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现WritableComparable接口,并实现compareTo方法
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private String mobile;
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    /**
     * 自定义排序规则
     * @param o
     * @return
     */
    public int compareTo(FlowBean o) {
        return this.sumFlow.compareTo(o.sumFlow);
    }

    /**
     * 序列化对象属性
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.mobile);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化属性
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.mobile = dataInput.readUTF();
        this.downFlow = dataInput.readLong();
        this.upFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    public void set(Long downFlow,Long upFlow){
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        this.sumFlow = downFlow + upFlow;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
