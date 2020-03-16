package com.rao.study.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String fileName;
    private Text pidKey = new Text();

    /**
     * 在任务调用前调用,只会调用一次
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取当前切片的信息
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //通过切片对象获取对应的文件名称
        fileName = fileSplit.getPath().getName();
    }

    /**
     * 任务调用方法,每行调用一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        TableBean tableBean = new TableBean();
        if (fileName.startsWith("order")) {
            String orderId = fields[0];
            String pid = fields[1];
            int amount = Integer.parseInt(fields[2]);
            tableBean.setOrderId(orderId);
            tableBean.setPid(pid);
            tableBean.setAmount(amount);
            tableBean.setFlag("order");
            tableBean.setPname("");
        }else{
            String pid = fields[0];
            String pname = fields[1];
            tableBean.setPid(pid);
            tableBean.setPname(pname);
            tableBean.setFlag("product");
            tableBean.setOrderId("");
            tableBean.setAmount(0);

        }
        pidKey.set(tableBean.getPid());
        //以pid作为key，则在shuffle阶段就会对pid进行分组处理
        context.write(pidKey,tableBean);
    }
}
