package com.rao.study.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer的开发,继承Reducer父类
 * 前两个泛型表示Mapper的输出参数类型
 * 后两个泛型表示Reducer输出的结果类型
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable(0);

    /**
     * 每个reduce都是一个reduceTask
     * @param key 单词字符串
     * @param values  表示对应key这个的所有值,Reducer会将Mapper输出的数据进行分组
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //计算这个key所对应的总数
        int sum = 0;
        for (IntWritable world:values){
            sum += world.get();
        }
        result.set(sum);
        //将每个reduceTask的结果输出到指定上下文中进行计算
        context.write(key,result);
    }
}
