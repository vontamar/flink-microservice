package com.example.model;

import java.io.Serializable;

public class AggregatedResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String key;
    private long count;
    private double sum;
    private double average;
    private double min;
    private double max;

    // Default constructor for deserialization
    public AggregatedResult() {
    }

    // Constructor used in EventAggregator.getResult()
    public AggregatedResult(String key, long count, double sum, double average, double min, double max) {
        this.key = key;
        this.count = count;
        this.sum = sum;
        this.average = average;
        this.min = min;
        this.max = max;
    }

    // Getters and setters
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "AggregatedResult{" +
                "key='" + key + '\'' +
                ", count=" + count +
                ", sum=" + sum +
                ", average=" + average +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}