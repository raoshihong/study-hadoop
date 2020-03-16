package com.rao.study.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class StudyCompress {

    @Test
    public void testCompress() throws Exception{
        Configuration configuration = new Configuration();
        //通过工厂模式创建对应的CompressionCodec实例
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        //获取解压缩对象
        //可以使用具体类创建,也可以通过工厂类来创建,还可以直接通过反射来创建
//        CompressionCodec codec = new BZip2Codec();
        //hadoop提供的ReflectionUtils
//        ReflectionUtils.newInstance(BZip2Codec.class,configuration);
        CompressionCodec codec = factory.getCodecByClassName(BZip2Codec.class.getName());

        String pathname = "/Users/raoshihong/work/my_test/order.txt";
        FileInputStream is = new FileInputStream(new File(pathname));

        FileOutputStream os = new FileOutputStream(new File(pathname+codec.getDefaultExtension()));
        //通过CompressionCodec将输出流包装成一个压缩输出流
        CompressionOutputStream cos = codec.createOutputStream(os);

        //进行流拷贝,并输出
        IOUtils.copyBytes(is,cos,1024*1024*5,true);

        IOUtils.closeStream(os);
    }

    @Test
    public void testDeCompress() throws Exception{
        Configuration configuration = new Configuration();
        //创建工厂
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        //通过工厂创建具体的CompressionCodec
        CompressionCodec codec = factory.getCodecByClassName(BZip2Codec.class.getName());
        //通过CompressionCodec创建解压流,读取压缩文件
        String pathname = "/Users/raoshihong/work/my_test/order.txt.bz2";
        FileInputStream fis = new FileInputStream(new File(pathname));
        CompressionInputStream cis = codec.createInputStream(fis);

        FileOutputStream fos = new FileOutputStream(new File(pathname.replace(codec.getDefaultExtension(),"")));

        //流拷贝
        IOUtils.copyBytes(cis,fos,1024*1024*5,true);

        IOUtils.closeStream(fis);
    }
}
