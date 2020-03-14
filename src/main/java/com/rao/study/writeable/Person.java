package com.rao.study.writeable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements Writable {

    private Float score;

    private Float sumScore;

    public Person() {
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        //将对象的字段进行序列化
        dataOutput.writeFloat(score);
        dataOutput.writeFloat(sumScore);
    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        //反序列化时,获取数据的顺序要与序列化时的顺序一致
        score = dataInput.readFloat();
        sumScore = dataInput.readFloat();
    }


    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }

    public Float getSumScore() {
        return sumScore;
    }

    public void setSumScore(Float sumScore) {
        this.sumScore = sumScore;
    }

    @Override
    public String toString() {
        return score+"\t"+sumScore;
    }
}
