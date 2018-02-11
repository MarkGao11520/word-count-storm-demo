package com.gwf.storm.stormai.function;

import com.gwf.storm.stormai.spout.LocalQueueEmitter;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * @author gaowenfeng
 */
@Slf4j
public class LocalQueueFunction<T> extends BaseFunction{


    private static final long serialVersionUID = -4190188350148882352L;
    private LocalQueueEmitter<T> emitter;

    public LocalQueueFunction(LocalQueueEmitter<T> emitter) {
        this.emitter = emitter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        T object = (T) tuple.get(0);
        log.debug("Queueing [{}]",object);
        this.emitter.enqueue(object);
    }
}
