package com.rao.study.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * 第一个泛型表示Mapper输出的key值类型 即相同sumFlow的FlowBean对象
 * 第二个泛型表示Mapper输出的LongWritable值类型  ,即每条数据的流量总和
 * 第三个泛型表示Reduce输出的key值类型 ,即手机号
 * 第四个泛型表示Reduce输出的value值类型 ,即每条数据的流量总和
 */
public class FlowReduce extends Reducer<FlowBean, LongWritable,Text, LongWritable> {
    private Text key_1 = new Text();

    @Override
    protected void reduce(FlowBean key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value:values){
            key_1.set(key.getMobile());
            context.write(key_1,value);
        }
    }
}
