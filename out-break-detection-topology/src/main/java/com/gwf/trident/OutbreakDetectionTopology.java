package com.gwf.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import com.gwf.trident.aggregator.Count;
import com.gwf.trident.filter.DiseaseFilter;
import com.gwf.trident.function.CityAssignment;
import com.gwf.trident.function.DispatchAlert;
import com.gwf.trident.function.HourAssignment;
import com.gwf.trident.function.OutbreakDetector;
import com.gwf.trident.spout.DiagnosisEventSpout;
import com.gwf.trident.state.OutbreakTrendFactory;

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

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("cdc",config,buildTopology());
        StormSubmitter.submitTopology("cdc",config,buildTopology());
//        Thread.sleep(200*1000);
//        cluster.shutdown();
    }
}
