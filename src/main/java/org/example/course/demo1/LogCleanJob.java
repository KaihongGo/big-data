package org.example.course.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class LogCleanJob extends Configured implements Tool {
    //设置静态输入路径及输出路径，也可以通过 args 方式将 两个路径作为参数传入，该路径为 hdfs路径，非宿主机（linux）路径
    //如使用args作为参数传入 INPUT_PATH ：args[0]  OUT_PAT:args[1]
    static final String INPUT_PATH = "hdfs://node1:8020/simple_demo/demo1/input_path/access_2013_05_30.log";
    static final String OUT_PATH = "hdfs://node1:8020/simple_demo/demo1/output_path";

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        try {
            int res = ToolRunner.run(conf, new LogCleanJob(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Job job = new Job(new Configuration(),
                LogCleanJob.class.getSimpleName());
        // 设置为可以打包运行
        job.setJarByClass(LogCleanJob.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        // 清理已存在的输出文件
        FileSystem fs = FileSystem.get(new URI(INPUT_PATH), getConf());
        Path outPath = new Path(OUT_PATH);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        boolean success = job.waitForCompletion(true);
        //如果清理数据成功输出 及 清理失败输出
        if (success) {
            System.out.println("Clean process success!");
        } else {
            System.out.println("Clean process failed!");
        }
        return 0;
    }
}