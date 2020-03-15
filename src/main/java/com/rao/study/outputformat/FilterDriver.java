package com.rao.study.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FilterDriver.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        //设置输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义到输出类
        job.setOutputFormatClass(FilterOutputFormat.class);

        //设置输入输出目录
        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        //提交job
        job.waitForCompletion(true);
    }
}
