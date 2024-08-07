/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.Utils;
import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.AbstractParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --parallelism 1 --kafka.partitions 1 "
 */
public class AdvertisingTopologyNative {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);


    public static void main(final String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);


        // Get the parameters of the benchmark from the command line arguments
        int parallelism = parameterTool.getInt("parallelism", 1);
        int kafkaPartitions = parameterTool.getInt("kafka.partitions", 1);

        // Get the remaining parameters from environment variables with default value
        String kafkaBroker = System.getenv().getOrDefault("KAFKA_BROKER", "kafka:9092");
        String zookeeperServer = System.getenv().getOrDefault("ZOOKEEPER_SERVER", "zookeeper:2181");
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "ad-events");
        int checkpointInterval = Integer.parseInt(System.getenv().getOrDefault("CHECKPOINT_INTERVAL", "1000"));

        // Create the Flink configuration in Map
        Map<String, String> conf = new HashMap<>();
        conf.put("topic", topic);
        conf.put("bootstrap.servers", kafkaBroker);
        conf.put("zookeeper.connect", zookeeperServer);
        conf.put("jedis_server", redisHost);
        conf.put("group.id", "myGroup");
        conf.put("kafka.partitions", String.valueOf(kafkaPartitions));
        conf.put("flink.checkpoint-interval", String.valueOf(checkpointInterval));

        // Create the FlinkBenchmarkParams object
        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(conf);


        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

		// Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));

        if(flinkBenchmarkParams.has("flink.checkpoint-interval")) {
            // enable checkpointing for fault tolerance
            env.enableCheckpointing(flinkBenchmarkParams.getLong("flink.checkpoint-interval", 1000));
        }

        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer<>(
                        topic,
                        new SimpleStringSchema(),
                        flinkBenchmarkParams.getProperties())).setParallelism(parallelism);

        messageStream
                .rebalance()
                // Parse the String as JSON
                .flatMap(new DeserializeBolt())

                //Filter the records if event type is "view"
                .filter(new EventFilterBolt())

                // project the event
                .<Tuple2<String, String>>project(2, 5)

                // perform join with redis data
                .flatMap(new RedisJoinBolt())

                // process campaign
                .keyBy(0)
                .flatMap(new CampaignProcessor());


        env.execute();
    }

    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<String, String, String, String, String, String, String>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("ad_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address"));
            out.collect(tuple);
        }
    }

    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

        RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            Map<String, String> globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
            ParameterTool parameterTool = ParameterTool.fromMap(globalParams);
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }

            Tuple3<String, String, String> tuple = new Tuple3<String, String, String>(
                    campaign_id,
                    (String) input.getField(0),
                    (String) input.getField(1));
            out.collect(tuple);
        }
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

        CampaignProcessorCommon campaignProcessorCommon;

        @Override
        public void open(Configuration parameters) {
            Map<String, String> globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
            ParameterTool parameterTool = ParameterTool.fromMap(globalParams);
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));

            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"));
            this.campaignProcessorCommon.prepare();
        }

        @Override
        public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(0);
            String event_time =  tuple.getField(2);
            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }
    }
}
