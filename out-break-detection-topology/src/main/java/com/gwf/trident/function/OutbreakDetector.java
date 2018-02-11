package com.gwf.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class OutbreakDetector extends BaseFunction {
    private static final long serialVersionUID = 3095252729347960017L;
    public static final int THRESHOLD = 10000;

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String key = tridentTuple.getString(0);
        Long count = tridentTuple.getLong(1);

        if(count>THRESHOLD){
            List<Object> values = new ArrayList<>();
            values.add("Outbreak detected for ["+key+"]");
            tridentCollector.emit(values);
        }
    }
}
