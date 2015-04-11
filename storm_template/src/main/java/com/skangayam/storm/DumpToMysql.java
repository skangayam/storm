package com.skangayam.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import java.util.ArrayList;
import java.util.List;

public class DumpToMysql {
    public static void main(String[] args) {
        /**
         * Configuring kafka-spout
         */
        BrokerHosts brokerHosts = new ZkHosts("localhost");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,
                "test_topic",  // topic name
                "/test_topic", // zoo keeper root
                "test_topic_consumer"); // consumer id

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        List<String> zkhosts = new ArrayList<String>();
        zkhosts.add("localhost");
        spoutConfig.zkServers = zkhosts;
        spoutConfig.zkPort = 2181;
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("mysqlBolt",
                new MysqlBolt(), 1)
                .shuffleGrouping("kafkaSpout");
        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("DumpToMysqlTopology", conf, builder.createTopology());
//            Utils.sleep(10000);
//            cluster.killTopology("DumpToMysqlTopology");
//            cluster.shutdown();
        }
    }
}