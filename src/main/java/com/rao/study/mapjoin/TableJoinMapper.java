package com.rao.study.mapjoin;

import com.rao.study.reducejoin.TableBean;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TableJoinMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {

    private Map<String,String> cache = new HashMap<>();
    private TableBean tableBean = new TableBean();

    /**
     * 在setup中获取缓存uri，在打开输入流读取缓存内容
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //在这里获取job事先缓存起来的小表数据
        URI[] uris = context.getCacheFiles();
        URI uri = uris[0];
        //开流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream is =  fileSystem.open(new Path(uri));
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        //解析每行数据
        String line = null;
        while ((line = reader.readLine())!=null){
            String[] fields = line.split("\t");

            //以pid为key,pname为value
            cache.put(fields[0],fields[1]);
        }
        //关闭输入流
        IOUtils.closeStream(reader);
        IOUtils.closeStream(is);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取大文件的切片后的每条数据
        String[] fields = value.toString().split("\t");
        String orderId = fields[0];
        String pid = fields[1];
        int amount = Integer.parseInt(fields[2]);
        tableBean.setOrderId(orderId);
        tableBean.setPname(cache.get(pid));
        tableBean.setAmount(amount);
        tableBean.setPid("");//没有值的需要设置为空字符串,不然序列号会报错
        tableBean.setFlag("");
        context.write(tableBean,NullWritable.get());
    }
}
