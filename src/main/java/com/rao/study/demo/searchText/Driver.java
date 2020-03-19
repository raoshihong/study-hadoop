package com.rao.study.demo.searchText;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Driver.class);
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/raoshihong/work/my_test/input"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/raoshihong/work/my_test/output"));

        boolean flag = job.waitForCompletion(true);
        if (flag) {
            Job job2 = Job.getInstance(new Configuration());
            job2.setJarByClass(Driver.class);
            job2.setMapperClass(SearchMapper2.class);
            job2.setReducerClass(SearchReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2,new Path("/Users/raoshihong/work/my_test/output"));
            FileOutputFormat.setOutputPath(job2,new Path("/Users/raoshihong/work/my_test/output2"));
            job2.waitForCompletion(true);
        }
    }
}
