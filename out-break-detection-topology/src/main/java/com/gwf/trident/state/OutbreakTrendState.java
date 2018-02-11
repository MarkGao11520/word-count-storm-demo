package com.gwf.trident.state;

import org.apache.storm.trident.state.map.NonTransactionalMap;

public class OutbreakTrendState extends NonTransactionalMap<Long>{
    protected OutbreakTrendState(OutbreakTrendBackingMap backing) {
        super(backing);
    }
}
