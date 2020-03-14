package com.rao.study.writeable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());

        //设置驱动类
        job.setJarByClass(ScoreDriver.class);

        //设置mapper和reducer类路径
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReduce.class);

        //设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Person.class);

        //设置Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        //设置文件输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置文件输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交job
        job.waitForCompletion(true);
    }
}
