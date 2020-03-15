package com.rao.study.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MobileMapper extends Mapper<LongWritable, Text,Text,Text> {
    private Text key_1 = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //将手机号作为key
        String mobile = fields[1];
        key_1.set(mobile);
        context.write(key_1,value);
    }
}
