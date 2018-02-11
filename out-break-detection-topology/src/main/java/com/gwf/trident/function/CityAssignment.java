package com.gwf.trident.function;

import com.gwf.trident.event.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * function只能添加tuple中的数据，不能变更和删除
 */
public class CityAssignment extends BaseFunction {
    private static final long serialVersionUID = -6839733009738894444L;

    private static final Logger LOG = LoggerFactory.getLogger(CityAssignment.class);

    private static Map<String,double[]> CITYES = new HashMap<>();

    {
        double[] phl = {39.875365,-75.249524};
        CITYES.put("PHL",phl);
        double[] nyc = {40.71448,-74.009524};
        CITYES.put("NYC",nyc);
        double[] sf = {-31.4250142,-62.0841809};
        CITYES.put("SF",sf);
        double[] la = {-34.05374,-118.24307};
        CITYES.put("LA",la);
    }


    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.getValue(0);
        double leastDistance = Double.MAX_VALUE;
        String closestCity = "NONE";

        for(Map.Entry<String,double[]> city:CITYES.entrySet()){
            double R = 6371; //km
            double x = (city.getValue()[0]-diagnosis.getLng())*Math.cos((city.getValue()[0]+diagnosis.getLng())/2);
            double y = (city.getValue()[1]-diagnosis.getLat());
            double d = Math.sqrt(x*x+y*y)*R;
            if(d<leastDistance){
                leastDistance = d;
                closestCity = city.getKey();
            }
        }

        List<Object> values = new ArrayList<>();
        values.add(closestCity);
        LOG.debug("Closest city to " +
                "lat=["+diagnosis.getLat()+"]," +
                "lng=["+diagnosis.getLng()+"]==" +
                "["+closestCity+"]," +
                "d="+"["+leastDistance+"]");
        tridentCollector.emit(values);
    }
}
