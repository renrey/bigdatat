package com.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordBean implements Writable {
    private long upload = 0;
    private long download = 0;
    private long sum = 0;

    // 必须有无参构造方法
    public RecordBean() {
    }

    public RecordBean(long upload, long download, long sum) {
        this.upload = upload;
        this.download = download;
        this.sum = sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 写入数据
        out.writeLong(upload);
        out.writeLong(download);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 读取按写入顺序读
        upload = in.readLong();
        download = in.readLong();
        sum = in.readLong();
    }

    public long getUpload() {
        return upload;
    }

    public long getDownload() {
        return download;
    }

    public long getSum() {
        return sum;
    }
}
