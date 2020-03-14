package com.rao.study.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 创建一个Maper实现类,继承Hadoop提供的Mapper父类
 * 在hadoop中需要使用Hadoop提供的类型比如Text类似java.lang.String
 * IntWritable类似java.lang.Integer
 *
 * 前两个泛型参数为读取文件的两个类型,行首编号和每行内容
 * 后两个泛型参数为map输出的数据类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private IntWritable intWritable = new IntWritable(1);
    private Text text = new Text();
    /**
     * 每个map都是一个mapTask
     * 实现map方法,按一行一行的读取
     * @param key  key表示读取的行首的偏移量
     * @param value value表示这行读取的内容
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取这行的内容
        String content = value.toString();
        //对这行进行单词分割
        String[] words = content.split(" ");
        //遍历数组,将数据转化为对应的格式交给mapper
        for (String word:words){
            text.set(word);
            //将map处理的结果输出到对应的上下文中
            context.write(text,intWritable);
        }
    }
}
