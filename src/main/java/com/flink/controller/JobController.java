package com.flink.controller;

import com.flink.model.Event;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final Map<String, JobID> runningJobs = new ConcurrentHashMap<>();

    @Value("${flink.jobmanager.host}")
    private String jobManagerHost;

    @Value("${flink.jobmanager.port}")
    private int jobManagerPort;

    @Value("${flink.kafka.topics.input}")
    private String inputTopic;

    public JobController(KafkaTemplate<String, Event> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, String>> getJobsStatus() throws Exception {
        Configuration config = new Configuration();
        config.setString(RestOptions.ADDRESS, jobManagerHost);
        config.setInteger(RestOptions.PORT, jobManagerPort);

        RestClusterClient<?> client = new RestClusterClient<>(config, "flink-client");

        Map<String, String> statusMap = new ConcurrentHashMap<>();

        for (Map.Entry<String, JobID> entry : runningJobs.entrySet()) {
            CompletableFuture<JobStatus> statusFuture = client.getJobStatus(entry.getValue());
            JobStatus status = statusFuture.get();
            statusMap.put(entry.getKey(), status.toString());
        }

        client.close();
        return ResponseEntity.ok(statusMap);
    }

    @PostMapping("/events")
    public ResponseEntity<String> publishEvent(@RequestBody Event event) {
        if (event.getId() == null) {
            event.setId(UUID.randomUUID().toString());
        }

        kafkaTemplate.send(inputTopic, event.getId(), event);
        return ResponseEntity.ok("Event published: " + event.getId());
    }

    @PostMapping("/{jobName}/deploy")
    public ResponseEntity<String> deployJob(@PathVariable String jobName, @RequestBody JobGraph jobGraph) {
        try {
            Configuration config = new Configuration();
            config.setString(RestOptions.ADDRESS, jobManagerHost);
            config.setInteger(RestOptions.PORT, jobManagerPort);

            RestClusterClient<?> client = new RestClusterClient<>(config, "flink-client");

            JobID jobId = client.submitJob(jobGraph).get();
            runningJobs.put(jobName, jobId);

            client.close();
            return ResponseEntity.ok("Job deployed with ID: " + jobId);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error deploying job: " + e.getMessage());
        }
    }

    @DeleteMapping("/{jobName}/cancel")
    public ResponseEntity<String> cancelJob(@PathVariable String jobName) {
        JobID jobId = runningJobs.get(jobName);
        if (jobId == null) {
            return ResponseEntity.notFound().build();
        }

        try {
            Configuration config = new Configuration();
            config.setString(RestOptions.ADDRESS, jobManagerHost);
            config.setInteger(RestOptions.PORT, jobManagerPort);

            RestClusterClient<?> client = new RestClusterClient<>(config, "flink-client");
            client.cancel(jobId).get();
            client.close();

            runningJobs.remove(jobName);
            return ResponseEntity.ok("Job canceled: " + jobId);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error canceling job: " + e.getMessage());
        }
    }
}

