package com.rao.study.groupcomparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置Reduce端的分组排序
        job.setGroupingComparatorClass(OrderGroupComparable.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        job.waitForCompletion(true);
    }
}
