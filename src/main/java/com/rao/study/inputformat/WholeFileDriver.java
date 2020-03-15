package com.rao.study.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WholeFileDriver {
    public static void main(String[] args) throws Exception{
        //创建job
        Job job = Job.getInstance(new Configuration());
        //指定驱动类
        job.setJarByClass(WholeFileDriver.class);

        //Mapper类可以不用指定,直接使用默认的Mapper和Reducer

        //设置Mapper出参
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //设置Reducer出参
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置自定义的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //设置OutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //设置输入文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
        job.waitForCompletion(true);
    }
}
