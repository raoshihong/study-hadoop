package com.rao.study.groupcomparable;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现自定义排序
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Float price;

    /**
     * 实现自定义排序方法
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        //先根据订单好进行排序
        int result = orderId.compareTo(o.getOrderId());
        if (result == 0) {
            //在根据价格排序
            result = o.getPrice().compareTo(price);
        }
        return result;
    }

    /**
     * 序列化对象属性
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeFloat(price);
    }

    /**
     * 反序列化对象属性
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readFloat();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
