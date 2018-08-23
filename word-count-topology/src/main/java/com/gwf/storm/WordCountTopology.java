package com.gwf.storm;

import com.gwf.storm.bolt.ReportBolt;
import com.gwf.storm.bolt.SplitSentenceBolt;
import com.gwf.storm.bolt.WordCountBolt;
import com.gwf.storm.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 拓扑
 * @author gaowenfeng
 */
public class WordCountTopology{

    private SentenceSpout spout;
    private SplitSentenceBolt splitBolt;
    private WordCountBolt countBolt;
    private ReportBolt reportBolt;

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "spilt-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    // at most once -- acker 0 at least once
    private static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        // 注册一个SentenceSpout 并赋值给其唯一的ID
        builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout(),2);

        // 注册一个SplitSentenceBolt,这个bolt订阅SentenceSpout发送的数据流
        // shuffleGrouping 随机均匀分发
        //parallelism_hint：线程数 setNumTasks：tasks数 task数：线程数->1：n
        builder.setBolt(SPLIT_BOLT_ID,new SplitSentenceBolt(),2)
                .setNumTasks(4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);

        // fieldsGrouping 将含有特定数据的tuple发送给特殊的bolt中
        builder.setBolt(COUNT_BOLT_ID,new WordCountBolt(),4)
                .fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));

        // globalGrouping 所有的tuple分发到唯一的bolt
        builder.setBolt(REPORT_BOLT_ID,new ReportBolt())
                .globalGrouping(COUNT_BOLT_ID);
        return builder.createTopology();
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        // 配置信息
        Config config = new Config();
        config.setNumAckers(0); // 设置NumAcker为0 就是at most once语义，此时代码调用ack，fail无效
        // config.setNumWorkers(2);   本地模式下只有一个JVM环节，设置worker数量没用

        // 本地集群

        //提交topology
        if(args.length==1){
            StormTopology stormTopology = buildTopology();
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, stormTopology);

        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME,config,buildTopology());
              Thread.sleep(10*1000);

            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }

        //杀死并关闭集群
    }

}
