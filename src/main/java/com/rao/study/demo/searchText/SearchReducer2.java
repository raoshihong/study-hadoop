package com.rao.study.demo.searchText;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SearchReducer2 extends Reducer<Text,Text,Text,Text> {
    private Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        for (Text text:values){
            buffer.append(text.toString()).append(" ");
        }
        v.set(buffer.toString());
        context.write(key,v);
    }
}
