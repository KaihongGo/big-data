package org.example.course.demo7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducers extends Reducer<Text, MyWriterble, Text, MyWriterble> {
    @Override
    protected void reduce(Text text, Iterable<MyWriterble> wt, Reducer<Text, MyWriterble, Text, MyWriterble>.Context context) throws IOException, InterruptedException {
        long UpPackNum = 0l;
        long DownPackNum = 0l;
        long UpPayLoad = 0l;
        long DownPayLoad = 0l;
        for (MyWriterble mt : wt) {
            UpPackNum = UpPackNum + mt.UpPackNum;
            DownPackNum = DownPackNum + mt.DownPackNum;
            UpPayLoad = UpPayLoad + mt.UpPayLoad;
            DownPayLoad = DownPayLoad + mt.DownPayLoad;
        }
        context.write(text, new MyWriterble(UpPackNum + "", DownPackNum + "", UpPayLoad + "", DownPayLoad + ""));
    }
}
