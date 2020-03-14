package com.rao.study.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

public class HDFSStudy {

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception{
        //通过hdfs客户端链接hdfs服务器,以搭建的hadoop102为例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"hadoop");
        //通过mkdir创建目录
        fileSystem.mkdirs(new Path("/opt/work"));
        //一定要记得关闭
        fileSystem.close();
    }


    /**
     * 从本地上传文件到hdfs文件系统中
     * @throws Exception
     */
    @Test
    public void testCopyFromLocalFile()throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"hadoop");
        fileSystem.copyFromLocalFile(new Path("/Users/raoshihong/work/script/"),new Path("/opt/"));
        fileSystem.close();
    }

    /**
     * 从hdfs文件系统中拷贝文件到本地
     * @throws Exception
     */
    @Test
    public void testCopyToLocalFile() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"hadoop");
        fileSystem.copyToLocalFile(new Path("/my_test"),new Path("/Users/raoshihong/work/"));
        fileSystem.close();
    }

    /**
     * 删除hdfs文件系统中的文件或文件夹
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"hadoop");
        //第二个参数表示是否递归删除
        fileSystem.delete(new Path("/my_output"),true);
        fileSystem.close();
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"hadoop");

        FSDataOutputStream os = fileSystem.create(new Path("/opt/work/test.txt"),true);

        os.writeUTF("这是中国,讲的是中文");

        fileSystem.close();
    }

    /**
     * 读取文件
     * @throws Exception
     */
    @Test
    public void testRead() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"hadoop");
        FSDataInputStream is = fileSystem.open(new Path("/opt/work/test.txt"));
        byte[] buff = new byte[1024];
        int length = -1;
        while ((length=is.read(buff))!=-1) {
            System.out.println(new String(buff,0,length));
        }
        fileSystem.close();
    }

    /**
     * 追加文件内容
     * @throws Exception
     */
    @Test
    public void testAppend() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean("dfs.support.append",true);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"hadoop");
        //append方法会返回一个文件的输出流对象
        FSDataOutputStream os = fileSystem.append(new Path("/opt/work/test.txt"),1024);
        os.writeUTF("hi，中国");
        fileSystem.close();
    }


}
