package com.idea.plumber.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class Start {

    public static class PushToMysql extends BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String x = tuple.getStringByField("fieldOne");
            persistToMysql(x);
            collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }

        private void persistToMysql(String message){
            try {
                Class.forName("com.mysql.jdbc.Driver");
                Connection connect = DriverManager
                        .getConnection("jdbc:mysql://localhost/plumber?"
                                + "user=plumber_user&password=plumber_password");
                PreparedStatement preparedStatement = connect.prepareStatement("insert into  plumber.stormtest values(?, ?)");
                preparedStatement.setLong(1,System.currentTimeMillis());
                preparedStatement.setString(2,message);
                preparedStatement.executeUpdate();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static class PassThroughBolt extends BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String x = tuple.getString(0);
            collector.emit(tuple, new Values(x));
            collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("fieldOne"));
        }
    }

    public static KafkaSpout getKafkaSpout(){
        BrokerHosts brokerHosts = new ZkHosts("sprfargas102.corp.intuit.net:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,
                "argos-parser",  // topic name
                "/argos-parser", // zoo keeper root
                UUID.randomUUID().toString()); // consumer id

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        return kafkaSpout;
    }

    public static void main(String[] args) {

        KafkaSpout kafkaSpout = getKafkaSpout();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("passThrough", new PassThroughBolt(), 1).shuffleGrouping("kafkaSpout");
        builder.setBolt("pushToMysql",new PushToMysql(), 1).shuffleGrouping("passThrough");

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
            cluster.submitTopology("HelloWorldTopology", conf, builder.createTopology());

//            Utils.sleep(10000);
//            cluster.killTopology("DumpToMysqlTopology");
//            cluster.shutdown();
        }
    }
}