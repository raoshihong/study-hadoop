package com.rao.study.demo.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean,Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //只有一个组,但是遍历values，表示每条记录都在遍历，所以value的值会改变(reduce输入原理)
        System.out.println("key="+key);
        int i=0;
        for (Text text:values){
            if(i<10) {
                context.write(text,key);
            }
            i++;
        }
    }
}
