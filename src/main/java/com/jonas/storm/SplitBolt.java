package com.jonas.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    //可以通过此方法获取用来发送 tuples 的 OutputCollector
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    //处理 tuples 和发送处理完成的 tuples
    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        String[] words = line.split("\t");
        for (String word : words) {
            collector.emit(new Values(word));
        }
    }

    //声明发送的 tuples 的名称，这样下一个组件才能知道如何接收
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
