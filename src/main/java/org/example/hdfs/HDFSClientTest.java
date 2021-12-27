package org.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class HDFSClientTest {
    private static Configuration conf = null;
    private static FileSystem fs = null;


    @Before
    public void connect2HDFS() throws IOException {
        // 设置客户端身份 以具备权限在hdfs上进行操作
        System.setProperty("HADOOP_USER_NAME", "root");

        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        fs = FileSystem.get(conf);
    }

    @Test
    public void mkdir() throws IOException {
        if (!fs.exists(new Path("/itheima2"))) {
            fs.mkdirs(new Path("/itheima2"));
        }
    }


    @After
    public void close() throws IOException {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
