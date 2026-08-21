package com.dmp.persistence.postgres;

import com.dmp.application.port.out.TenantRepository;
import com.dmp.domain.tenant.Tenant;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Base for PostgreSQL integration tests.
 *
 * <p>Runs against a real PostgreSQL with the real Flyway migrations applied. Nothing here would
 * catch what these tests catch if it ran against H2: the JSONB columns, the GIN index, the
 * immutability triggers and the {@code ILIKE} searches are all PostgreSQL behaviour, and a
 * substitute database would pass while production failed.
 *
 * <p>The container is static, so one instance is shared by every subclass in the module rather
 * than started per class.
 */
@Testcontainers
@SpringBootTest(classes = {PostgresTestApplication.class, AbstractPostgresIT.TestConfig.class})
public abstract class AbstractPostgresIT {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("dmp")
            .withUsername("dmp")
            .withPassword("dmp")
            .withReuse(true);

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.enabled", () -> true);
        // Validate rather than create: the point is to prove the entity mappings match the
        // migrations. Letting Hibernate generate the schema would test it against itself.
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
    }

    @Autowired
    protected TenantRepository tenants;

    protected TenantId tenantId;

    @BeforeEach
    void createTenant() {
        // Every query is tenant-scoped, so a tenant must exist before anything else can.
        Tenant tenant = tenants.save(Tenant.create(
                "test-" + System.nanoTime() % 1_000_000,
                "Test tenant",
                Instant.parse("2026-08-07T00:00:00Z")));
        this.tenantId = tenant.id();
    }

    @org.springframework.boot.test.context.TestConfiguration
    static class TestConfig {

        /** Fixed clock, so assertions about timestamps are deterministic. */
        @org.springframework.context.annotation.Bean
        Clock clock() {
            return Clock.fixed(Instant.parse("2026-08-07T00:00:00Z"), ZoneOffset.UTC);
        }
    }
}
