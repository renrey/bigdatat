package com.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, RecordBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<RecordBean> values, Reducer<Text, RecordBean, Text, Text>.Context context) throws IOException, InterruptedException {
        Long uploadSum = 0L;
        Long downloadSum = 0L;
        Long totalSum = 0L;
        for (RecordBean value : values) {
            uploadSum += value.getUpload();
            downloadSum += value.getDownload();
            totalSum += value.getSum();
        }
        String outputString = String.join(" ", String.valueOf(uploadSum), String.valueOf(downloadSum), String.valueOf(totalSum));
        context.write(key, new Text(outputString));
    }
}
