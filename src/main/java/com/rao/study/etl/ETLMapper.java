package com.rao.study.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //过滤数据
        if(!value.toString().contains("192.196.100.1")){
            //使用hadoop中的计算器
            context.getCounter("ETL","success").increment(1);
            context.write(value,NullWritable.get());
        }else{
            context.getCounter("ETL","fail").increment(1);
        }
    }
}
