package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FlinkMicroserviceApplication {
    public static void main(String[] args) {
        SpringApplication.run(FlinkMicroserviceApplication.class, args);
    }
}