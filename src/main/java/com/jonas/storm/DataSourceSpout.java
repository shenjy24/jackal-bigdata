package com.jonas.storm;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;

public class DataSourceSpout extends BaseRichSpout {

    private final Random random = new Random();
    private final List<String> words = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");

    private SpoutOutputCollector spoutOutputCollector;

    //可以通过此方法获取用来发送 tuples 的 SpoutOutputCollector
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    //必须在此方法内部发送 tuples
    @Override
    public void nextTuple() {
        String lineData = productData();
        spoutOutputCollector.emit(new Values(lineData));
        Utils.sleep(1000);
    }

    //声明发送的 tuples 的名称，这样下一个组件才能知道如何接受
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    private String productData() {
        Collections.shuffle(words);
        int endIndex = random.nextInt(words.size()) % (words.size()) + 1;
        return StringUtils.join(words.toArray(), "\t", 0, endIndex);
    }
}
