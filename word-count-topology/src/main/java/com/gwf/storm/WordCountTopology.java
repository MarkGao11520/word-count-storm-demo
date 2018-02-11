package com.gwf.storm;

import com.gwf.storm.bolt.SplitSentenceBolt;
import com.gwf.storm.bolt.WordCountBolt;
import com.gwf.storm.spout.SentenceSpout;
import com.gwf.storm.bolt.ReportBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * 拓扑
 * @author gaowenfeng
 */
@Component
public class WordCountTopology implements CommandLineRunner{

    @Autowired
    private SentenceSpout spout;
    @Autowired
    private SplitSentenceBolt splitBolt;
    @Autowired
    private WordCountBolt countBolt;
    @Autowired
    private ReportBolt reportBolt;

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "spilt-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    private StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        // 注册一个SentenceSpout 并赋值给其唯一的ID

        builder.setSpout(SENTENCE_SPOUT_ID, spout,2);

        // 注册一个SplitSentenceBolt,这个bolt订阅SentenceSpout发送的数据流
        // shuffleGrouping 随机均匀分发
        //parallelism_hint：线程数 setNumTasks：tasks数 task数：线程数->1：n
        builder.setBolt(SPLIT_BOLT_ID,splitBolt,2)
                .setNumTasks(4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);

        // fieldsGrouping 将含有特定数据的tuple发送给特殊的bolt中
        builder.setBolt(COUNT_BOLT_ID,countBolt,4)
                .fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));

        // globalGrouping 所有的tuple分发到唯一的bolt
        builder.setBolt(REPORT_BOLT_ID,reportBolt)
                .globalGrouping(COUNT_BOLT_ID);
        return builder.createTopology();
    }

    @Override
    public void run(String... strings) throws Exception {
        // 配置信息
        Config config = new Config();
        // config.setNumWorkers(2);   本地模式下只有一个JVM环节，设置worker数量没用

        // 本地集群

        //提交topology
        if(strings.length==1){
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, buildTopology());
//            Thread.sleep(10*1000);
        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME,config,buildTopology());
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }

        //杀死并关闭集群

    }
}
