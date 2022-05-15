package com.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, RecordBean> {


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, RecordBean>.Context context) throws IOException, InterruptedException {
        // 分割当前行字符
        String[] strArray = value.toString().split("\t");
        String phone = strArray[1];// 电话
        String uploadStr = strArray[8];// 上行
        String downloadStr = strArray[9];// 下行
        MapWritable mapWritable = new MapWritable();
        Long uploadNum = Long.valueOf(uploadStr);
        Long downloadNum = Long.valueOf(downloadStr);
        Long sum = uploadNum + downloadNum;

        RecordBean recordBean = new RecordBean(uploadNum, downloadNum, sum);
        context.write(new Text(phone), recordBean);
    }
}
