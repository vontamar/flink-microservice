package com.flink.service;

import com.flink.model.AggregatedResult;
import com.flink.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


@Service
public class KafkaService {

    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final List<AggregatedResult> latestResults = new CopyOnWriteArrayList<>();

    @Value("${flink.kafka.topics.input}")
    private String inputTopic;

    public KafkaService(KafkaTemplate<String, Event> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendEvent(Event event) {
        kafkaTemplate.send(inputTopic, event.getId(), event);
    }

    @KafkaListener(topics = "${flink.kafka.topics.output}", groupId = "${flink.kafka.consumer.group-id}")
    public void consumeResults(String message) {
        try {
            AggregatedResult result = objectMapper.readValue(message, AggregatedResult.class);

            // Store the latest results (keep only last 10)
            latestResults.add(0, result);
            if (latestResults.size() > 10) {
                latestResults.remove(latestResults.size() - 1);
            }

        } catch (Exception e) {
            // Log error handling the message
            System.err.println("Error processing Kafka message: " + e.getMessage());
        }
    }

    public List<AggregatedResult> getLatestResults() {
        return new ArrayList<>(latestResults);
    }
}
