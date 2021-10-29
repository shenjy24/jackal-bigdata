package com.jonas.hadoop.wordcount;

import com.jonas.util.WordCountDataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.net.URL;

/**
 * 组装 MapReduce 作业，并提交到服务器运行
 */
public class WordCountApp {

    private static final String HDFS_URL = "hdfs://192.168.157.128:8020";
    private static final String HADOOP_USER_NAME = "root";

    public static void main(String[] args) throws Exception {
        URL inUrl = WordCountApp.class.getClassLoader().getResource("input.txt");
        new WordCountApp().doCountWorkJob(inUrl, "/word/output");
    }

    private void doCountWorkJob(URL inPath, String outPath) throws Exception {
        // 需要指明 hadoop 用户名，否则在 HDFS 上创建目录时可能会抛出权限不足的异常
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);

        Configuration configuration = new Configuration();
        // 指明 HDFS 的地址
        configuration.set("fs.defaultFS", HDFS_URL);

        // 创建一个 Job
        Job job = Job.getInstance(configuration);
        // 设置运行的主类
        job.setJarByClass(WordCountApp.class);
        // 设置 Mapper 和 Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 设置 Mapper 输出 key 和 value 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置 Reducer 输出 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置 Combiner
        job.setCombinerClass(WordCountReducer.class);
        // 设置自定义分区规则
        job.setPartitionerClass(WordCountPartitioner.class);
        // 设置 reduce 个数
        job.setNumReduceTasks(WordCountDataUtil.WORDS.size());

        // 如果输出目录已经存在，则必须先删除，否则重复运行程序时会抛出异常
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER_NAME);
        Path outputPath = new Path(outPath);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        // 设置作业输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(inPath.toURI()));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 将作业提交到群集并等待它完成，参数设置为true代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);

        // 关闭之前创建的fileSystem
        fileSystem.close();

        // 根据作业结果,终止当前运行的Java虚拟机,退出程序
        System.exit(result ? 0 : -1);
    }
}
