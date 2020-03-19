package com.rao.study.demo.searchText;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SearchMapper2 extends Mapper<LongWritable, Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("--");
        k.set(fields[0]);
        v.set(fields[1].replace("\t","-->"));
        context.write(k,v);
    }
}
