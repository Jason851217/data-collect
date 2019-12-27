package com.huaxin.datacollect;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataCollectApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataCollectApplication.class, args);
    }
}
