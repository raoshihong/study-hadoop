package com.rao.study.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MobileDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MobileDriver.class);

        job.setMapperClass(MobileMapper.class);
        job.setReducerClass(MobileReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置numberReduceTask,注意numberReduceTask不能设置大于分区数,否则
        job.setNumReduceTasks(5);
        //设置自定义的分区类
        job.setPartitionerClass(MyPartition.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        //提交任务
        job.waitForCompletion(true);
    }
}
