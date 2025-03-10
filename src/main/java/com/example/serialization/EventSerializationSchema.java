package com.example.serialization;

import com.example.model.AggregatedResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;

public class EventSerializationSchema implements SerializationSchema<AggregatedResult> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(AggregatedResult result) {
        try {
            return objectMapper.writeValueAsBytes(result);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing AggregatedResult", e);
        }
    }
}