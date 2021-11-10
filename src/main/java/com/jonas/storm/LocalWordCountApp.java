package com.jonas.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 通过 TopologyBuilder 将定义好的组件进行串联形成 Topology，并提交到本地集群（LocalCluster）运行
 */
public class LocalWordCountApp {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("DataSourceSpout", new DataSourceSpout());

        // 指明将 DataSourceSpout 的数据发送到 SplitBolt 中处理
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");

        // 指明将 SplitBolt 的数据发送到 CountBolt 中 处理
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        // 创建本地集群用于测试 这种模式不需要本机安装 storm,直接运行该 Main 方法即可
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountApp", new Config(), builder.createTopology());
    }
}
