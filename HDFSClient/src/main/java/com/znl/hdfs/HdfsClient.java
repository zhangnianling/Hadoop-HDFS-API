package com.znl.hdfs;
/*

*1.获取客户端对象
*2.执行相关操作命令
*3.关闭资源

HDFS  Zookeeper

* */

import jdk.nashorn.internal.ir.IfNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.TrustAnchor;
import java.util.Arrays;

public class HdfsClient {


    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri;
        uri = new URI("hdfs://hadoop102:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");
        //用户
        String user = "znl";
        //获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //关闭资源
        fs.close();
    }

    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {
        //创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    //上传
    /*
    参数优先级
    hdfs-default.xml  ==>  hdfs.site.xml  ==>  在项目资源目录下配置文件优先级高  ==>  代码里配置最高
    * */
    @Test
    public void testPut() throws IOException {
        //参数1 删除原数据
        //参数2 是否允许覆盖
        //参数3 原数据路径
        //参数4 目的地址路径
        fs.copyFromLocalFile(false, true, new Path("D:\\sunwukong.txt"), new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

    //文件下载
    @Test
    public void testGet() throws IOException {
        //参数1 源文件是否删除
        //参数2 源文件路径HDFS
        //参数3 目标地址路径Windows
        //参数4 文件校验CRC
        fs.copyToLocalFile(true, new Path("hdfs://hadoop102/xiyou/huaguoshan"), new Path("D:\\sunwukong"), true);
    }

    //删除
    @Test
    public void testRm() throws IOException {
        //参数1 要删除的路径
        //参数2 是否递归删除
        // fs.delete(new Path("/jinguo"),true);
        fs.delete(new Path("/xiyou"), false);
    }

    //文件更名或移动
    @Test
    public void testMove() throws IOException {
        //参数1 源文件路径
        //参数2 目标文件路径

        //文件名称修改
        //fs.rename(new Path("/input/word.txt"),new Path("/input/znl.txt"));
        //文件移动并更名
        //fs.rename(new Path("/input/znl.txt"),new Path("/cls.txt"));
        //目录的更名
        fs.rename(new Path("/input"), new Path("/output1"));
    }

    //获取文件详细信息
    @Test
    public void testFileDetail() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("==========" + fileStatus.getPath() + "==========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        //目标路径
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        //循环获取
        for (FileStatus status : listStatus) {
            //判断
            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("目录：" + status.getPath().getName());
            }

        }
    }
}
