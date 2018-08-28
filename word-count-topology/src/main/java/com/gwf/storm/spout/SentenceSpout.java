package com.gwf.storm.spout;

import com.gwf.storm.bolt.ReportBolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gaowenfeng
 */
public class SentenceSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private ConcurrentHashMap<UUID,Values> pending;
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
        this.pending = new ConcurrentHashMap<>();
    }

    /**
     * 由ISpout接口定义，所有spout实现的核心所在
     * Storm通过调用这个方法向输出的collector发射tuple
     */
    @Override
    public void nextTuple() {
        ReportBolt.map.put(100000000+new Random().nextInt(100000000),"");
        Values values = new Values(sentences[index]);
        UUID msdId = UUID.randomUUID();
        //为每个tuple设置唯一标识
        this.pending.put(msdId,values);
        //发射tuple
        this.collector.emit(values,msdId);
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

    /**
     * 下游bolt将tuple处理成功，会调用ack方法
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    /**
     * 下游bolt将tuple处理失败，会调用msgId方法
     * @param msgId 每个tuple的唯一标识
     */
    @Override
    public void fail(Object msgId) {
        System.out.println("emit Fail,msgId : "+msgId);
        this.collector.emit(this.pending.get(msgId),msgId);
    }
}
