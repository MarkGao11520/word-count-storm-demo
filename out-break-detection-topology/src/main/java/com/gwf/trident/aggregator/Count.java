package com.gwf.trident.aggregator;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class Count implements CombinerAggregator<Long>{
    private static final long serialVersionUID = -5346057794609578344L;

    @Override
    public Long init(TridentTuple tridentTuple) {
        return 1L;
    }

    @Override
    public Long combine(Long aLong, Long t1) {
        return aLong+t1;
    }

    @Override
    public Long zero() {
        return 0L;
    }
}
