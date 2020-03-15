package com.rao.study.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 实现FileOutputFormat
 */
public class FilterOutputFormat extends FileOutputFormat<Text, NullWritable> {
    /**
     * 重写数据输出方法
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FilterRecordWriter filterRecordWriter = new FilterRecordWriter();
        filterRecordWriter.initialize(context);
        return filterRecordWriter;
    }
}
