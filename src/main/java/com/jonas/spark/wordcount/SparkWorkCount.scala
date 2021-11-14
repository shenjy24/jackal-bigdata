package com.jonas.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkWorkCount {
  def main(args: Array[String]) {
    // 使用的是本地模式，配置为 local[2]，这里不能配置为 local[1]。
    // 这是因为对于流数据的处理，Spark 必须有一个独立的 Executor 来接收数据，然后再由其他的 Executors 来处理
    // 所以为了保证数据能够被处理，至少要有 2 个 Executors。
    val sparkConf = new SparkConf().setAppName("SparkWorkCount").setMaster("local[2]")
    // Spark 流处理本质是将流数据拆分为一个个批次，然后进行微批处理，batchDuration 就是批次拆分的时间间隔
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    // 创建文本输入流,并进行词频统计 (本地环境测试)
    val lines = streamingContext.socketTextStream("hadoop001", 9999)
    lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()
    // 启动服务
    streamingContext.start()
    // 等待服务结束
    streamingContext.awaitTermination()
  }
}
