package com.flink.serialization;

import com.flink.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class EventDeserializationSchema implements DeserializationSchema<Event> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Event.class);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeExtractor.getForClass(Event.class);
    }
}