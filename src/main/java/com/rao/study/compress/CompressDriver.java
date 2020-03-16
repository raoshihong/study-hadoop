package com.rao.study.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompressDriver {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //这里的conf设置跟直接在core-site.xml,mapred-site.xml中配置的是一样的
        conf.setBoolean("mapreduce.map.output.compress",true);
        conf.set("mapreduce.map.output.compress.codec",BZip2Codec.class.getName());
        conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
        conf.set("mapreduce.output.fileoutputformat.compress.codec",BZip2Codec.class.getName());

        Job job = Job.getInstance(conf);
        job.setJarByClass(CompressDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        //可以通过FileOutputFormat设置解压缩方式,表示在Reduce输出阶段进行数据解压缩
        //启用解压缩

//        FileOutputFormat.setCompressOutput(job,true);
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        job.waitForCompletion(true);
    }
}
