package com.gwf.trident.spout;

import com.gwf.trident.event.DiagnosisEvent;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DiagnosisEventEmitter implements ITridentSpout.Emitter<Long>,Serializable {
    private static final long serialVersionUID = 7403463322182725035L;
    AtomicInteger successfulTrasactions = new AtomicInteger(0);

    /**
     * 这里发射的tuple是BatchCoordinator类初始化的一批数据
     * @param transactionAttempt 事务的信息
     * @param aLong batch的元信息
     * @param tridentCollector Emitter用来发送tuple的collector
     */
    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, Long aLong, TridentCollector tridentCollector) {
        System.out.println("emitBatch->start,aLong="+aLong+" date="+new Date().getTime());
        for(int i=0;i<10000;i++){
            // tuple
            List<Object> events = new ArrayList<>();
            // event 数据
            double lat = new Double(-30+(int)(Math.random()*75));
            double lng = new Double(-120+(int)(Math.random()*70));
            long time = System.currentTimeMillis();
            String diag = new Integer(320+(int)(Math.random()*7)).toString();
            DiagnosisEvent event = new DiagnosisEvent(lat,lng,time,diag);
            events.add(event);
            tridentCollector.emit(events);
        }
        System.out.println("emitBatch->end,date="+new Date().getTime());
    }

    @Override
    public void success(TransactionAttempt transactionAttempt) {
        successfulTrasactions.incrementAndGet();
    }

    @Override
    public void close() {

    }
}
