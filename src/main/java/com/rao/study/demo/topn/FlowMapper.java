package com.rao.study.demo.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean, Text> {
    private FlowBean flowBean = new FlowBean();
    private Text v_1 = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String mobile = fields[0];
        v_1.set(mobile);
        flowBean.set(Long.parseLong(fields[1]),Long.parseLong(fields[2]));
        context.write(flowBean,v_1);
    }
}
