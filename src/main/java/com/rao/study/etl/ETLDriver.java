package com.rao.study.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETLDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduceTasks 任务数为0,也可以不用设置,即筛选后在reduce中进行其他业务计算
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        job.waitForCompletion(true);
    }
}
