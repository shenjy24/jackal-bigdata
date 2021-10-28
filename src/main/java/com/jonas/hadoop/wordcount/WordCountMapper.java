package com.jonas.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN : mapping 输入 key 的类型，即每行的偏移量 (每行第一个字符在整个文本中的位置)；Long 类型，对应 Hadoop 中的 LongWritable 类型；
 * VALUEIN : mapping 输入 value 的类型，即每行数据；String 类型，对应 Hadoop 中 Text 类型；
 * KEYOUT ：mapping 输出的 key 的类型，即每个单词；String 类型，对应 Hadoop 中 Text 类型；
 * VALUEOUT：mapping 输出 value 的类型，即每个单词出现的次数；这里用 int 类型，对应 IntWritable 类型。
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将每行数据按照指定分隔符进行拆分
        String[] words = value.toString().split("\t");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
