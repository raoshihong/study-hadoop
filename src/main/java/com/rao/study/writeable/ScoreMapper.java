package com.rao.study.writeable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 继承Mapper
 * 在泛型中使用我们自定义的类
 */
public class ScoreMapper extends Mapper<LongWritable, Text,Text,Person> {

    private Text key_1 = new Text();
    private Person person = new Person();

    /**
     * 实现方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String content = value.toString();
        String[] texts = content.split(" ");

        //取出读取到的每个值
        String name = texts[0];
        String course = texts[1];
        Float score = Float.valueOf(texts[2]);

        //设置key的值
        key_1.set(name);//第一个为用户名称

        person.setScore(score);
        person.setSumScore(0.0f);

        context.write(key_1,person);

    }
}
