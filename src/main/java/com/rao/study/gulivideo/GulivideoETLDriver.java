package com.rao.study.gulivideo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class GulivideoETLDriver implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(configuration);

        //设置jar
        job.setJarByClass(GulivideoETLDriver.class);

        //设置mapper
        job.setMapperClass(GulivideoETLMapper.class);

        //设置mapper参数
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        boolean result = job.waitForCompletion(true);

        return result?1:0;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        try {
            //使用ToolRunner运行
            ToolRunner.run(new Configuration(),new GulivideoETLDriver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
