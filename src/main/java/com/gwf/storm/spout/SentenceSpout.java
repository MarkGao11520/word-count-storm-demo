package com.gwf.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index =0;

    /**
     * 由ISpout接口定义，所有Spout组件在初始化时调用这个方法
     * @param map  包含Storm配置信息的map
     * @param topologyContext  topology组件的信息
     * @param spoutOutputCollector  提供发射的方法
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * 由ISpout接口定义，所有spout实现的核心所在
     * Storm通过调用这个方法向输出的collector发射tuple
     */
    @Override
    public void nextTuple() {
        //发射tuple
        this.collector.emit(new Values(sentences[index]));
        index++;
        if(index >= sentences.length){
            index = 0;
        }
        Utils.sleep(1);
    }

    /**
     * IComponent接口定义，所以Storm组件（spout，bolt）必须实现
     * Storm通过这个方法告诉Storm该组件会发射那些数据流，每个数据流的tuple包含那些字段
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
