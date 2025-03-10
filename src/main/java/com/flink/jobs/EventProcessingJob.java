package com.flink.jobs;

import com.flink.model.AggregatedResult;
import com.flink.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class EventProcessingJob {
    private static final Logger logger = LoggerFactory.getLogger(EventProcessingJob.class);

    private final StreamExecutionEnvironment env;
    private final ParameterTool parameters;

    @Autowired
    public EventProcessingJob(StreamExecutionEnvironment env, ParameterTool parameters) {
        this.env = env;
        this.parameters = parameters;
    }

    // Use ApplicationReadyEvent instead of PostConstruct to ensure all beans are fully initialized
    @EventListener(ApplicationReadyEvent.class)
    public void startJob() {
        try {
            logger.info("Initializing Flink job with parameters: {}", parameters.toMap());

            // Configure Kafka consumer
            Properties consumerProps = new Properties();
            consumerProps.setProperty("bootstrap.servers", parameters.get("bootstrap.servers", "localhost:9092"));
            consumerProps.setProperty("group.id", parameters.get("group.id", "flink-microservice-group"));

            logger.info("Kafka consumer properties: {}", consumerProps);

            // In a real deployment, we would set up the Flink job here
            // For testing purposes, we'll just log the configuration
            logger.info("Flink job configuration complete - would start job in production environment");

            // Comment out the actual job execution for troubleshooting
            /*
            // Create Kafka source
            FlinkKafkaConsumer<Event> kafkaSource = new FlinkKafkaConsumer<>(
                    parameters.get("input-topic"),
                    new EventDeserializationSchema(),
                    consumerProps
            );
            
            // Create Kafka sink
            FlinkKafkaProducer<AggregatedResult> kafkaSink = new FlinkKafkaProducer<>(
                    parameters.get("output-topic"),
                    new EventSerializationSchema(),
                    consumerProps
            );

            // Define the processing pipeline
            DataStream<Event> inputStream = env.addSource(kafkaSource)
                    .name("Kafka-Source");
            
            // Filter out invalid events
            DataStream<Event> validEvents = inputStream
                    .filter(new ValidEventFilter())
                    .name("Valid-Event-Filter");
            
            // Enrich the events if needed
            DataStream<Event> enrichedEvents = validEvents
                    .map(new EventEnricher())
                    .name("Event-Enricher");
            
            // Process events with stateful operations
            DataStream<Event> processedEvents = enrichedEvents
                    .keyBy(new EventTypeKeySelector())
                    .process(new StatefulEventProcessor())
                    .name("Stateful-Processor");
            
            // Aggregate events in time windows
            DataStream<AggregatedResult> aggregatedResults = processedEvents
                    .keyBy(event -> event.getType())
                    .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                    .aggregate(new EventAggregator(), new EventWindowProcessor())
                    .name("Event-Aggregator");
            
            // Send results to Kafka
            aggregatedResults.addSink(kafkaSink)
                    .name("Kafka-Sink");
            
            // Execute the job
            env.execute("Event Processing Microservice");
            */
        } catch (Exception e) {
            logger.error("Error starting Flink job", e);
        }
    }

    // Filter for valid events
    public static class ValidEventFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) {
            return event != null
                    && event.getId() != null
                    && event.getType() != null
                    && event.getTimestamp() != null;
        }
    }

    // Enrich events with additional data
    public static class EventEnricher implements MapFunction<Event, Event> {
        @Override
        public Event map(Event event) {
            // Here you could enrich the event with external data
            // For example, add location data, user info, etc.
            return event;
        }
    }

    // Key selector by event type
    public static class EventTypeKeySelector implements KeySelector<Event, String> {
        @Override
        public String getKey(Event event) {
            return event.getType();
        }
    }

    // Stateful processing for events
    public static class StatefulEventProcessor extends KeyedProcessFunction<String, Event, Event> {
        private ValueState<Double> lastValueState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "last-value", Double.class);
            lastValueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
            // Get the last value for this key
            Double lastValue = lastValueState.value();

            // Update the state with the current value
            lastValueState.update(event.getValue());

            // If there was a previous value, we could do some comparison
            if (lastValue != null) {
                // Example: Detect significant changes
                double change = Math.abs((event.getValue() - lastValue) / lastValue) * 100;
                if (change > 10.0) {
                    // Flag this event as a significant change
                    event.setPayload(event.getPayload() + " (significant change: " + change + "%)");
                }
            }

            // Forward the possibly modified event
            out.collect(event);
        }
    }

    // Accumulator for the aggregation
    public static class EventAccumulator {
        long count = 0;
        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        public void add(double value) {
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
    }

    // Aggregation function for events
    public static class EventAggregator implements AggregateFunction<Event, EventAccumulator, AggregatedResult> {
        @Override
        public EventAccumulator createAccumulator() {
            return new EventAccumulator();
        }

        @Override
        public EventAccumulator add(Event event, EventAccumulator acc) {
            acc.add(event.getValue());
            return acc;
        }

        @Override
        public AggregatedResult getResult(EventAccumulator acc) {
            double average = acc.count > 0 ? acc.sum / acc.count : 0;
            return new AggregatedResult(null, acc.count, acc.sum, average, acc.min, acc.max);
        }

        @Override
        public EventAccumulator merge(EventAccumulator a, EventAccumulator b) {
            EventAccumulator merged = new EventAccumulator();
            merged.count = a.count + b.count;
            merged.sum = a.sum + b.sum;
            merged.min = Math.min(a.min, b.min);
            merged.max = Math.max(a.max, b.max);
            return merged;
        }
    }

    // Window processing function
    public static class EventWindowProcessor extends ProcessWindowFunction<AggregatedResult, AggregatedResult, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<AggregatedResult> elements, Collector<AggregatedResult> out) {
            // There should be only one element since we're using an AggregateFunction
            AggregatedResult result = elements.iterator().next();

            // Set the key for the result
            result.setKey(key);

            // Output the result
            out.collect(result);
        }
    }
}
