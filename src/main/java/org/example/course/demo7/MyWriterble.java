package org.example.course.demo7;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWriterble implements Writable {

    long UpPackNum;
    long DownPackNum;
    long UpPayLoad;
    long DownPayLoad;

    public MyWriterble() {
    }

    public MyWriterble(String UpPackNum, String DownPackNum, String UpPayLoad, String DownPayLoad) {
        this.UpPackNum = Long.parseLong(UpPackNum);
        this.DownPackNum = Long.parseLong(DownPackNum);
        this.UpPayLoad = Long.parseLong(UpPayLoad);
        this.DownPayLoad = Long.parseLong(DownPayLoad);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(UpPackNum);
        out.writeLong(DownPackNum);
        out.writeLong(UpPayLoad);
        out.writeLong(DownPayLoad);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.UpPackNum = in.readLong();
        this.DownPackNum = in.readLong();
        this.UpPayLoad = in.readLong();
        this.DownPayLoad = in.readLong();
    }

    @Override
    public String toString() {
        return UpPackNum + "\t" + DownPackNum + "\t" + UpPayLoad + "\t" + DownPayLoad;
    }
}
