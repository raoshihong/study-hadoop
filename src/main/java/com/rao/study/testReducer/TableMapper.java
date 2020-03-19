package com.rao.study.testReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 0000001 Pdt_01 222.8
 * 0000001 Pdt_02 33.8
 * 0000002 Pdt_05 722.4
 * 0000002	Pdt_03	522.8
 * 0000002	Pdt_04	122.4
 * 0000003	Pdt_06	232.8
 * 0000003	Pdt_02	33.8
 */

public class TableMapper extends Mapper<LongWritable, Text,Text, OrderBean> {
    private OrderBean orderBean = new OrderBean();
    private Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        orderBean.setPname(fields[1]);
        orderBean.setAmount(Float.parseFloat(fields[2]));
        k.set(fields[0]);
        context.write(k,orderBean);
    }
}
