package com.rao.study.demo.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 继承WritableComparator,进行分组排序
 */
public class FlowGroupComparable extends WritableComparator {
    public FlowGroupComparable() {
        super(FlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;//直接返回0,表示所有的数据都分到一个组
    }
}
