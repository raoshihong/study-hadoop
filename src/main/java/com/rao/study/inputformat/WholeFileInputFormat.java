package com.rao.study.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;

/**
 * 自定义文件输入处理类,<Text,BytesWritable> BytesWritable表示value为二进制数据
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    /**
     * 是否需要分片,不分片则可以不用重写自己的分片规则方法getSplits，如果要按照自己分片规则，则需要重写getSplits方法
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 将分片数据转化为key-value形式返回给Mapper
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //使用自定义RecordReader,返回自定义的key-value数据交给map处理
        return new WholeFileRecordReader();
    }

}
