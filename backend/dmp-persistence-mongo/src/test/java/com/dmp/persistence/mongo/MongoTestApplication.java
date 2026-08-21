package com.dmp.persistence.mongo;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

/** Minimal Spring Boot context for exercising the MongoDB adapter against a real replica set. */
@SpringBootApplication
public class MongoTestApplication {

    @Bean
    Clock clock() {
        return Clock.fixed(Instant.parse("2026-08-07T00:00:00Z"), ZoneOffset.UTC);
    }
}
