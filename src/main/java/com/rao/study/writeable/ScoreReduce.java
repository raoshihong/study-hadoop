package com.rao.study.writeable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReduce extends Reducer<Text,Person,Text, Person> {

    private Person sumScore = new Person();

    @Override
    protected void reduce(Text key, Iterable<Person> values, Context context) throws IOException, InterruptedException {
        float sum = 0.0f;
        for (Person value : values) {
            sum += value.getScore();
        }
        sumScore.setSumScore(sum);
        context.write(key,sumScore);
    }
}
