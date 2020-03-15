package com.rao.study.groupcomparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 定义排序类,实现WritableComparator
 */
public class OrderGroupComparable extends WritableComparator{
    public OrderGroupComparable() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean orderBeanA = (OrderBean)a;
        OrderBean orderBeanB = (OrderBean)b;
        // 根据订单好排序进行分组排序
        return orderBeanA.getOrderId().compareTo(orderBeanB.getOrderId());
    }
}
