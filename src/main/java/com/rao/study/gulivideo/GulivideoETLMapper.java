package com.rao.study.gulivideo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class GulivideoETLMapper extends Mapper<LongWritable, Text, NullWritable,Text> {

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //对每行数据处理
        //1.先将所有的数据以\t分隔
        String[] fileds = value.toString().split("\t");
        if (fileds.length<9) {
            return;
        }
        //2.将category列的数据删除空格,只以&拼接
        fileds[3] = fileds[3].replace(" ","");
        //3.将fields以'\t'拼接,而related ids的数据以&拼接
        StringBuilder builder = new StringBuilder();
        for(int i=0;i<fileds.length;i++){
            if (i<9) {
                if (i==fileds.length-1) {
                    builder.append(fileds[i]);
                }else{
                    builder.append(fileds[i]+"\t");
                }
            }else{//大于等于9的为related ids
                if (i==fileds.length-1) {
                    builder.append(fileds[i]);
                }else{
                    builder.append(fileds[i]+"&");
                }
            }
        }
        text.set(builder.toString());
        context.write(NullWritable.get(),text);
    }
}
