package com.rao.study.combine;

import com.rao.study.mapreduce.WordCountMapper;
import com.rao.study.mapreduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerDriver {
    public static void main(String[] args) throws Exception{
        //1.创建一个mapreduce 的job对象
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(CombinerDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //设置combine
        job.setCombinerClass(WordCountReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/application/hadoop-2.10.0/etc/hadoop/"));

        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output/"));

        job.waitForCompletion(true);
    }
}
