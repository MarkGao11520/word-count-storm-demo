package com.gwf.function;

import backtype.storm.tuple.Values;
import com.gwf.common.EWMA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;



public class MovingAverageFuncation extends BaseFunction {
    private static final long serialVersionUID = -7299613133744911672L;
    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageFuncation.class);

    private EWMA ewma;
    private EWMA.Time emitRatePer;

    public MovingAverageFuncation(EWMA ewma, EWMA.Time emitRatePer) {
        this.ewma = ewma;
        this.emitRatePer = emitRatePer;
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        // 更新当前的平均速率
        this.ewma.mark(tridentTuple.getLong(0));
        LOG.debug("Rate: {}",this.ewma.getAverageRatePer(this.emitRatePer));
        // 发送当前的平均值
        tridentCollector.emit(new Values(this.ewma.getAverageRatePer(emitRatePer)));
    }
}
