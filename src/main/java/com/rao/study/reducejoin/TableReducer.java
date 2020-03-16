package com.rao.study.reducejoin;

import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.List;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //相同pid的作为一组,所以order.txt中的和pd.txt中相同的pid的数据会分到一组
        //key就是pid
        List<TableBean> orderBeans = Lists.newArrayList();
        String pname = "";
        for (TableBean tableBean:values){
            if (tableBean.getFlag().equals("order")) {
                TableBean bean = new TableBean();
                try{
                    BeanUtils.copyProperties(bean,tableBean);
                }catch (Exception e){

                }
                orderBeans.add(bean);
            }else{
                pname = tableBean.getPname();
            }
        }

        for(TableBean orderBean : orderBeans){
            orderBean.setPname(pname);
            //将合并的数据输出
            context.write(orderBean,NullWritable.get());
        }

    }
}
