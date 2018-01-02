package com.gwf.trident.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.math.BigDecimal;
import java.util.Map;

/**
 * spout不做真正的发射tuple，而是把工作分解给了BatchCoordinator，Emitter
 */
public class DiagnosisEventSpout implements ITridentSpout<Long>{
    private static final long serialVersionUID = -759614302465814707L;
    SpoutOutputCollector collector;
    BatchCoordinator<Long> coordinator = new DefaultCoordinator();
    Emitter<Long> emitter = new DiagnosisEventEmitter();

    /**
     * 负责管理批次和元数据
     * @param s
     * @param map
     * @param topologyContext
     * @return
     */
    @Override
    public BatchCoordinator<Long> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return coordinator;
    }

    /**
     * 依靠元数据来恰当地进行批次的数据重放
     * @param s
     * @param map
     * @param topologyContext
     * @return
     */
    @Override
    public Emitter<Long> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    /**
     * 声明发射的字段
     * @return
     */
    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }


}
