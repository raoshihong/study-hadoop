package com.rao.study.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner的泛型参数为Mapper输出的参数类型
 */
public class MyPartition extends Partitioner<Text,Text> {

    public int getPartition(Text key, Text value, int numPartitions) {
        String mobile = key.toString();
        //根据手机号区分分区
        switch (mobile.substring(0,3)){
            case "138":
                return 0;
            case "139":
                return 1;
            case "135":
                return 2;
            case "134":
                return 3;
            default:
                return 4;
        }
    }
}
