package com.gwf.storm.stormai.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * @author gaowenfeng
 */
public class LocalQueueSpout<T> implements ITridentSpout<Long> {
    private static final long serialVersionUID = -759614302465814707L;
    private BatchCoordinator<Long> coordinator;
    private Emitter<Long> emitter;

    public LocalQueueSpout(BatchCoordinator<Long> coordinator, Emitter<Long> emitter) {
        this.coordinator = coordinator;
        this.emitter = emitter;
    }


    @Override
    public BatchCoordinator<Long> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("gamestate");
    }
}
