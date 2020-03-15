package com.rao.study.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 第一个泛型LongWritable表示读取行的行首偏移量
 * 第二个泛型Text表示读取的这行数据
 * 第三个泛型FlowBean 表示Mapper输出每行的key
 * 第四个泛型LongWritable 表示Mapper输出的流量总和
 */
public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,LongWritable> {
    private FlowBean flowBean = new FlowBean();
    private LongWritable value_1 = new LongWritable();

    /**
     * 对每行进行调度,所以context.write 会输出相同key，
     * 不同value的数据到环形缓冲区中,因为FlowBean实现了排序接口,所以会自动进行排序判断
     * 在shuffle阶段会将相同key的数据进行分组排序
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //解析每行数据
        String[] fields = value.toString().split("\t");
        flowBean.setMobile(fields[1]);
        flowBean.set(Long.valueOf(fields[3]),Long.valueOf(fields[4]));
        //将key-value输出到环形缓冲区中
        value_1.set(flowBean.getSumFlow());
        context.write(flowBean,value_1);
    }
}
