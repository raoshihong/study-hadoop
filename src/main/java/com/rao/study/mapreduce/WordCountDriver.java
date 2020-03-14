package com.rao.study.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception{
        //1.创建一个mapreduce 的job对象
        Job job = Job.getInstance(new Configuration());

        //2.设置job入口的类路径
        job.setJarByClass(WordCountDriver.class);

        //3.设置job对应的Mapper类路径
        job.setMapperClass(WordCountMapper.class);

        //4.设置job对应的Reducer类路径
        job.setReducerClass(WordCountReduce.class);

        //5.设置Mapper出参key的类路径
        job.setMapOutputKeyClass(Text.class);

        //6.设置Mapper出参value的类路径
        job.setMapOutputValueClass(IntWritable.class);

        //7.设置Reducer出参key的类路径
        job.setOutputKeyClass(Text.class);

        //8.设置Reducer出参value的类路径
        job.setOutputValueClass(IntWritable.class);

        //9.设置输入文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //10.设置输出文件路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //11.提交任务
        job.waitForCompletion(true);
    }
}
