package com.rao.study.testReducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TableReducer extends Reducer<Text,OrderBean,Text,OrderBean> {
    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        for (OrderBean orderBean:values){
            context.write(key,orderBean);
        }
    }
}
