package com.gwf.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.gwf.trident.aggregator.Count;
import com.gwf.trident.filter.DiseaseFilter;
import com.gwf.trident.function.CityAssignment;
import com.gwf.trident.function.DispatchAlert;
import com.gwf.trident.function.HourAssignment;
import com.gwf.trident.function.OutbreakDetector;
import com.gwf.trident.spout.DiagnosisEventSpout;
import com.gwf.trident.state.OutbreakTrendFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class OutbreakDetectionTopology {
    public static StormTopology buildTopology(){
        TridentTopology tridentTopology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = tridentTopology.newStream("event",spout);
        inputStream
                .each(new Fields("event"),new DiseaseFilter())
                .each(new Fields("event"),new CityAssignment(),new Fields("city"))
                .each(new Fields("event","city"),new HourAssignment(),new Fields("hour","cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(new OutbreakTrendFactory(),new Count(),new Fields("count"))
                .newValuesStream()
                .each(new Fields("cityDiseaseHour","count"),new OutbreakDetector(),new Fields("alert"))
                .each(new Fields("alert"),new DispatchAlert(),new Fields());
        return tridentTopology.build();
    }

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc",config,buildTopology());
        Thread.sleep(200*1000);
        cluster.shutdown();
    }
}
