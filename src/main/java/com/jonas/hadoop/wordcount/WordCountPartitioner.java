package com.jonas.hadoop.wordcount;

import com.jonas.util.WordCountDataUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return WordCountDataUtil.WORDS.indexOf(text.toString());
    }

}
