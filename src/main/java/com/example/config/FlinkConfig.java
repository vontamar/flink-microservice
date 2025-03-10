package com.example.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class FlinkConfig {

    @Value("${flink.jobmanager.host}")
    private String jobManagerHost;

    @Value("${flink.jobmanager.port}")
    private int jobManagerPort;

    @Value("${flink.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${flink.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${flink.kafka.topics.input}")
    private String inputTopic;

    @Value("${flink.kafka.topics.output}")
    private String outputTopic;

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // Enable checkpointing
        env.enableCheckpointing(60000);

        return env;
    }

    @Bean
    public ParameterTool flinkParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("bootstrap.servers", bootstrapServers);
        parameters.put("group.id", consumerGroupId);
        parameters.put("input-topic", inputTopic);
        parameters.put("output-topic", outputTopic);

        return ParameterTool.fromMap(parameters);
    }
}
