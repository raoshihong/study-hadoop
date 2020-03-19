package com.rao.study.testReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Driver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OrderBean.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/input"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        job.waitForCompletion(true);
    }
}
