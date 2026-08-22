package com.dmp.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * The platform's single deployable artifact (ADR-0004).
 *
 * <p>Three profiles select the role:
 * <ul>
 *   <li>{@code control-plane} — REST API, pipeline management, scheduling, run orchestration</li>
 *   <li>{@code worker} — plugin loading, DAG execution, connectors, checkpointing</li>
 *   <li>{@code all} — both, for local development only</li>
 * </ul>
 *
 * <p>Docker Compose and Kubernetes run this same image as two deployments. The roles communicate
 * only through Kafka and the datastores, never through in-process calls, even under the
 * {@code all} profile — enforced by an ArchUnit rule rather than by discipline. That is what makes
 * splitting into two artifacts later a build-file change rather than a refactor.
 */
@SpringBootApplication(scanBasePackages = {
        "com.dmp.app",
        "com.dmp.application",
        "com.dmp.persistence",
        "com.dmp.connector.runtime",
        "com.dmp.engine",
        "com.dmp.recordlog",
        "com.dmp.ratelimit",
        "com.dmp.events",
        "com.dmp.transform"
})
@ConfigurationPropertiesScan(basePackages = "com.dmp")
@EnableTransactionManagement
// Required by RunReaper, whose sweep is what stops a dead pod's chunks or a stopped run from
// stalling forever. Without it the beans exist and nothing ever calls them.
@EnableScheduling
// Entities and repositories live in their own module, outside this application's
// package, so both scans are declared explicitly rather than inferred.
@EntityScan(basePackages = "com.dmp.persistence.postgres.entity")
@EnableJpaRepositories(basePackages = "com.dmp.persistence.postgres.repository")
public class DmpApplication {

    public static void main(String[] args) {
        SpringApplication.run(DmpApplication.class, args);
    }
}
