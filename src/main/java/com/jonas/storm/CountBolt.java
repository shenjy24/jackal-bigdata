package com.jonas.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    private final Map<String, Integer> counts = new HashMap<>();

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        int count = counts.getOrDefault(word, 0);
        counts.put(word, count + 1);

        System.out.print("当前实时统计结果:");
        counts.forEach((key, value) -> System.out.print(key + ":" + value + "; "));
        System.out.println();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
