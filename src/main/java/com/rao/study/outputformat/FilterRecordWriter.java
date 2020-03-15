package com.rao.study.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义RecordWriter类,对数据记录进行操作
 * 对应对泛型要跟Reducer输出对泛型一致
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream ip10Os;
    private FSDataOutputStream ip12Os;
    private static final String ip10 = "192.168.100.10";
    private static final String ip12 = "192.168.100.12";

    /**
     * 初始化,将任务TaskAttemptContext传入
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(TaskAttemptContext context)throws IOException, InterruptedException{
        //通过FileSystem获取输出流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //获取输出路径，这个路径在Driver中到FileOutputFormat时设置
        String path = context.getConfiguration().get(FileOutputFormat.OUTDIR);
        this.ip10Os = fileSystem.create(new Path(path+"/"+ip10+".log"));
        this.ip12Os = fileSystem.create(new Path(path+"/"+ip12+".log"));
    }


    /**
     * 对输出对每行key-value进行处理
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断reduce输出的每行key-value进行处理
        String content = key.toString()+"\n";
        if (key.toString().contains(ip10)) {
            //输出到目标目录下
            this.ip10Os.write(content.getBytes());
        }else{
            this.ip12Os.write(content.getBytes());
        }
    }

    /**
     * 关闭操作
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(this.ip10Os);
        IOUtils.closeStream(this.ip12Os);
    }
}
