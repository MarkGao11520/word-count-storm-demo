package com.gwf.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * 实现上报bolt
 * @author gaowenfeng
 */
public class ReportBolt extends BaseRichBolt {
    private HashMap<String,Long> counts = null;
    public static final HashMap<Integer,String> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        ReportBolt.map.put(1400000000+new Random().nextInt(100000000),"");
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word,count);
        System.out.println(word+" : "+count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    /**
     * 由IBolt接口定义，Storm在终止一个bolt前调用这个方法
     * 本利中利用cleanup()方法在关闭topology时输出最后的计数结果
     */
    @Override
    public void cleanup() {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for(String key:keys){
            System.out.println(key+" : "+this.counts.get(key));
        }
        System.out.println("-------------------");
    }
}
