package com.skangayam.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Start {

    static Producer<String, String> producer;

    public static void main(String[] args) throws InterruptedException {
        configureKafkaProducer();
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Trending", conf, buildTopology(drpc));
        Thread.sleep(2000);
        while (true) {
            drpc.execute("StreamingCount", "apple,sony,samsung");
            Thread.sleep(3000);
        }
    }

    public static void configureKafkaProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    private static StormTopology buildTopology(LocalDRPC drpc) {
        OpaqueTridentKafkaSpout kafkaSpout = getKafkaSpout();
        TridentTopology topology = new TridentTopology();

        TridentState keyCounts = topology.newStream("tweetSpout", kafkaSpout)
                .each(kafkaSpout.getOutputFields(), new SplitIntoTuples(), new Fields("tag", "tweetMessage"))
                .groupBy(new Fields("tag"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("tag"), new Count(), new Fields("partialCount"))
                .parallelismHint(1);

        topology.newDRPCStream("StreamingCount", drpc)
                .each(new Fields("args"), new Split(), new Fields("userRequestedKey"))
                .stateQuery(keyCounts, new Fields("userRequestedKey"), new MapGet(), new Fields("totalCount"))
                .each(new Fields("totalCount"), new FilterNull())
                .each(new Fields("userRequestedKey", "totalCount"), new PersistToKafka())
                .each(new Fields("userRequestedKey", "totalCount"), new Print());

        return topology.build();
    }

    public static OpaqueTridentKafkaSpout getKafkaSpout() {
        // Configuring kafka-spout
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "Tweets");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
        return spout;
    }

    public static class Print extends BaseFilter {

        private static final long serialVersionUID = 1L;

        public boolean isKeep(TridentTuple tuple) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Object obj : tuple.getValues()) {
                if (obj instanceof String) {
                    stringBuilder.append((String) obj);
                } else if (obj instanceof Long) {
                    stringBuilder.append((Long) obj);
                }
                stringBuilder.append(" ");
            }
            System.out.println("Key: " + tuple.getString(0) + ", Count: " + tuple.getLong(1));
            return true;
        }
    }

    public static class SplitIntoTuples extends BaseFunction {

        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String, String> map = new HashMap<String, String>();
            ObjectMapper mapper = new ObjectMapper();

            //convert JSON string to Map
            try {
                map = mapper.readValue(tuple.getString(0), new TypeReference<HashMap<String, String>>() {
                });
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Split the input Json to tuples hashTag and message
            collector.emit(new Values(map.get("hashTag"), map.get("message")));
        }
    }

    public static class Split extends BaseFunction {

        public void execute(TridentTuple tuple, TridentCollector collector) {
            String[] keys = tuple.getString(0).split(",");
            for (String key : keys) {
                collector.emit(new Values(key));
            }
        }
    }

    public static class PersistToKafka extends BaseFilter {
        private void writeToTopic(String key, Long count) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("tweet_counts", null, key + "-" +
                                                                                                String.valueOf(count));
            producer.send(data);
        }

        public boolean isKeep(TridentTuple tuple) {
            writeToTopic(tuple.getString(0), tuple.getLong(1));
            return true;
        }
    }
}
