package com.gwf.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 实现计数Bolt
 * @author gaowenfeng
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String,Long> counts = null;



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        ReportBolt.map.put(1200000000+new Random().nextInt(100000000),"");
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(null == count){
            count = 0L;
        }
        count++;
        this.counts.put(word,count);
        this.collector.emit(tuple,new Values(word,count));
    //    this.collector.fail(tuple);
    }
}
