package com.gwf.trident.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class DispatchAlert extends BaseFunction {
    private static final long serialVersionUID = -67239656492616604L;
    private static final Logger LOG = LoggerFactory.getLogger(DispatchAlert.class);


    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String alert = tridentTuple.getString(0);
        LOG.error("ALERT RECEIVED ["+alert+"]");
        LOG.error("Dispatch the national guard!");
        System.exit(0);
    }
}
