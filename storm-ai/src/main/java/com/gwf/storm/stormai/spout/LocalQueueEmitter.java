package com.gwf.storm.stormai.spout;

import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内存队列发射器
 * @author gaowenfeng
 */
@Slf4j
public class LocalQueueEmitter<T> implements ITridentSpout.Emitter<Long> {
    private static final int MAX_BATCH_SIZE = 1000;
    private static AtomicInteger successfulTransactions = new AtomicInteger(0);
    private static Map<String,BlockingDeque<Object>> queues = new HashMap<>();
    private String queueName;

    public LocalQueueEmitter(String queueName) {
        queues.put(queueName,new LinkedBlockingDeque<>());
        this.queueName = queueName;
    }

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        int size = 0;
        log.debug("Getting batch for [{}]",tx.getTransactionId());
        while (null!=getQueue().peek() && size<=MAX_BATCH_SIZE){
            List<Object> values = new ArrayList<>();
            try {
                log.debug("Waiting on work from [{}]:[{}]",this.queueName,getQueue().size());
                values.add(getQueue().take());
                log.debug("Got work from [{}]:[{}]",this.queueName,getQueue().size());
            }catch (InterruptedException ex){
                log.error("线程中断异常",ex);
            }
            collector.emit(values);
            size++;
        }
        log.info("Emitted [{}] elements in [{}],[{}] remain in queue",size,tx.getTransactionId(),getQueue().size());
    }

    public void enqueue(T work){
        log.debug("Adding work to [{}]:[{}]",this.queueName,getQueue().size());
        if(getQueue().size() % MAX_BATCH_SIZE == 0){
            log.info("[{}] size=[{}].",this.queueName,getQueue().size());
            this.getQueue().add(work);
        }
    }

    @Override
    public void success(TransactionAttempt transactionAttempt) {

    }

    @Override
    public void close() {

    }

    private BlockingDeque<Object> getQueue(){
        return LocalQueueEmitter.queues.get(this.queueName);
    }
}
