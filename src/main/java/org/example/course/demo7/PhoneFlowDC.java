package org.example.course.demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class PhoneFlowDC {
    public static void main(String[] args) throws Exception {
        final String INPUT_PATHs = "hdfs://node1:8020/simple_demo/demo7/input_path";
        final String OUT_PATHs = "hdfs://node1:8020/simple_demo/demo7/output_path";
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "yarn");
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATHs), conf);
        final Path outPath = new Path(OUT_PATHs);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        Job job = new Job(new Configuration(), PhoneFlowDC.class.getSimpleName());
        // 设置为可以打包运行
        job.setJarByClass(PhoneFlowDC.class);
        // 1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, INPUT_PATHs);
        // 指定哪个类用来格式化输入文件
        job.setInputFormatClass(TextInputFormat.class);
        // 1.2指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);
        // 指定输出<k2,v2>的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyWriterble.class);
        // 1.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        // 2.2 指定自定义的reduce类
        job.setReducerClass(MyReducers.class);
        // 指定输出<k3,v3>的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyWriterble.class);
        // 2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATHs));
        // 设定输出文件的格式化类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
    }
}
