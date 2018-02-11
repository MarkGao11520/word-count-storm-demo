package com.gwf.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class OutbreakTrendFactory implements StateFactory
{
    private static final long serialVersionUID = 1747560776666612048L;

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        return new OutbreakTrendState(new OutbreakTrendBackingMap());
    }
}
