package com.rao.study.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

/**
 * 自定义切片后的key-value的数据
 * 并重写对应的方法,这些方法都会自动被MapReduce框架调用
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean noRead;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FileSplit fileSplit;
    private FSDataInputStream is;

    /**
     * 初始化方法,只会被调用一次
     * @param split 表示每一个分片数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //对文件处理,直接强制转化为FileSplit
        fileSplit = (FileSplit)split;
        //获取文件对路径Path
        Path path = fileSplit.getPath();
        //获取对应的文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //打开输入流
        is = fileSystem.open(path);
    }

    /**
     * 进行下一个key-value对读取,返回是否还有下一条,读取不到则返回false
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {

        //没有读,则进行文件读取
        if(noRead){
            //进行文件的读取
            byte[] buff = new byte[(int) fileSplit.getLength()];
            is.read(buff);

            //设置key
            key.set(fileSplit.getPath().toString());
            //设置value为读取到的文件的内容
            value.set(buff,0,buff.length);

            noRead = true;
            return true;
        }else{
            return false;
        }

    }

    /**
     * 获取当前对key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前对value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        //对整个文件对读取,则进度值有两种 0/1  0表示未读取,1表示已读取,如果是对文件内容逐步读取,则这里需要进行进度对计算
        return noRead?0:1;
    }

    public void close() throws IOException {
        // 关闭流
        IOUtils.closeStream(is);
    }
}
