package com.gwf.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Random;

/**
 * 实现语句分割bolt
 * @author gaowenfeng
 */
public class SplitSentenceBolt extends BaseRichBolt{

    private OutputCollector collector;

    /**
     * 由IBolt接口定义，类同与ISpout接口的open方法
     * 在bolt初始化的时候调用，可以用来准备bolt用到的资源，如数据库连接
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /**
     * bolt的核心功能所在，由IBolt接口定义
     * 每当从订阅的数据流中接收一个tuple，都会调用这个方法
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        ReportBolt.map.put(1300000000+new Random().nextInt(100000000),"");
        //读取SentenceSpout发送的sentence值
        String sentence = tuple.getStringByField("sentence");
        //分发计数
        String[] words = sentence.split(" ");
        for(String word:words){
            //将当前要发射的tuple和接收的tuple锚定
            this.collector.emit(tuple,new Values(word));
        }
        //当前bolt将tuple处理成功，需要调用ack方法
        this.collector.ack(tuple);
        //当前bolt将tuple处理失败，需要调用fail方法
      //   this.collector.fail(tuple);
     //   throw new RuntimeException("");


    }

    /**
     * IComponent接口定义，所以Storm组件（spout，bolt）必须实现
     * Storm通过这个方法告诉Storm该组件会发射那些数据流，每个数据流的tuple包含那些字段
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
