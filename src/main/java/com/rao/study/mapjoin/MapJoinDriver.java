package com.rao.study.mapjoin;

import com.rao.study.reducejoin.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class MapJoinDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(TableJoinMapper.class);
        //不需要设置reduce
        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //将指定的文件内容进行缓存
        job.setCacheFiles(new URI[]{new URI("file:///Users/raoshihong/work/product.txt")});

        //设置ReduceTasks任务数为0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        job.waitForCompletion(true);
    }
}
