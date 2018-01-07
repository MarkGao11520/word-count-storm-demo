package com.gwf.spout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.gwf.common.NotifyMessageMapper;
import com.gwf.filter.BooleanFilter;
import com.gwf.common.EWMA;
import com.gwf.function.JsonProjectFunction;
import com.gwf.function.MovingAverageFuncation;
import com.gwf.function.ThresholdFilterFunction;
import com.gwf.function.XMPPFunction;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class LogAnalysisTopology {

    private static StormTopology buildTopology(){
        TridentTopology topology = new TridentTopology();
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(buildConfig());
        Stream spoutStream = topology.newStream("kafka-stream",spout);

        // 1：接受并解析JSON格式的原始数据
        // 2：提前并且发射需要的字段
        Fields jsonFields = new Fields("level","timestamp","message","logger");
        Stream parsedStream = spoutStream.each(
                new Fields("str"),
                new JsonProjectFunction(jsonFields),
                jsonFields);

        // 3：更新指数移动平均值
        EWMA ewma = new EWMA().sliding(1.0, EWMA.Time.MINUTES).withAlpha(EWMA.ONE_MINUTE_ALPHA);  //指定一个1分钟的滑动窗口
        Stream averageStream = parsedStream.each(
                new Fields("timestamp"),
                new MovingAverageFuncation(ewma, EWMA.Time.MINUTES),
                new Fields("average")
        );

        // 4：判断移动平均值是否超过了特定的阈值
        ThresholdFilterFunction tff = new ThresholdFilterFunction(50D);
        Stream thresholdStream = averageStream.each(new Fields("average"),tff,new Fields("change","threshold"));

        // 5.过滤除了状态变更之外的事件
        Stream filteredStream = thresholdStream.each(new Fields("change"),new BooleanFilter());

        filteredStream.each(filteredStream.getOutputFields(),new XMPPFunction(new NotifyMessageMapper()),new Fields());
        return topology.build();
    }


    private static TridentKafkaConfig buildConfig(){
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        Broker broker = new Broker("localhost",9092);
        globalPartitionInformation.addPartition(0,broker);
        BrokerHosts kafkaHosts = new StaticHosts(globalPartitionInformation);   //静态指定kafka主机
        // BrokerHosts brokerHosts = new ZkHosts("localhost:2181");  //从zookeeper中动态获取kafka主机

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts,"mytopic");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.startOffsetTime = -1;
        return spoutConf;
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
//        conf.put(XMPPFunction.XMPP_USER,"1152057576@qq.com");
//        conf.put(XMPPFunction.XMPP_PASSWORD,"gaoFENG123");
//        conf.put(XMPPFunction.XMPP_SERVER,"qq.com");
//        conf.put(XMPPFunction.XMPP_TO,"2573088731@qq.com");

        conf.setMaxSpoutPending(5);
        if(args.length == 0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("log-analysis",conf,buildTopology());
        }else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],conf,buildTopology());
        }
    }



}
