package com.gwf.trident.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class OutbreakTrendFactory implements StateFactory
{
    private static final long serialVersionUID = 1747560776666612048L;

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        return new OutbreakTrendState(new OutbreakTrendBackingMap());
    }
}
