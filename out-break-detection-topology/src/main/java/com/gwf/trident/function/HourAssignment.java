package com.gwf.trident.function;

import com.gwf.trident.event.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class HourAssignment extends BaseFunction {
    private static final long serialVersionUID = -6377230762510343694L;
    private static final Logger LOG = LoggerFactory.getLogger(HourAssignment.class);


    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.getValue(0);
        String city = tridentTuple.getString(1);

        long timestamp = diagnosis.getTime();
        long hourSinceEpoch = timestamp/1000/60/60;

        LOG.debug("key= [ "+city+":"+hourSinceEpoch+"]");
        String key = city + ":" +diagnosis.getDiagnosisCode()+":"+hourSinceEpoch;
        List<Object> values = new ArrayList<>();
        values.add(hourSinceEpoch);
        values.add(key);
        tridentCollector.emit(values);
    }
}
