package com.example.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.io.Serializable;
import java.time.Instant;

public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String type;
    private String payload;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "UTC")
    private Instant timestamp;
    private double value;

    // Default constructor for Jackson deserialization
    public Event() {
    }

    // All-args constructor used by the create method
    public Event(String id, String type, String payload, Instant timestamp, double value) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.timestamp = timestamp;
        this.value = value;
    }

    // For Flink serialization
    public static Event create(String id, String type, String payload, double value) {
        return new Event(id, type, payload, Instant.now(), value);
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", payload='" + payload + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
