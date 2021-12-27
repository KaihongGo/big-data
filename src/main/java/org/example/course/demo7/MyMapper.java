package org.example.course.demo7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MyMapper extends Mapper<LongWritable, Text, Text, MyWriterble> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String[] line = value.toString().split("\t+");
        if (line.length == 21) {
            // 数据并不规范，列数不同，需要清理
            String UpPackNum = line[21];
            String DownPackNum = line[22];
            String UpPayLoad = line[23];
            String DownPayLoad = line[24];
            MyWriterble my = new MyWriterble(UpPackNum, DownPackNum, UpPayLoad, DownPayLoad);
            context.write(new Text(line[2]), my);
        } else {
            context.write(new Text(line[2]), new MyWriterble("", "", "", ""));
        }
    }
}