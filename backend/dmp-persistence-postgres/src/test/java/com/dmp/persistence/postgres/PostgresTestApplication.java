package com.dmp.persistence.postgres;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Minimal Spring Boot context for exercising the PostgreSQL adapter in isolation.
 *
 * <p>Deliberately does not start the real application. These tests verify one adapter against one
 * real database; pulling in the web layer, MongoDB and the whole application context would make
 * them slower and would blur what a failure is telling you.
 */
@SpringBootApplication
@EnableJpaRepositories(basePackages = "com.dmp.persistence.postgres.repository")
@EntityScan(basePackages = "com.dmp.persistence.postgres.entity")
public class PostgresTestApplication {
}
