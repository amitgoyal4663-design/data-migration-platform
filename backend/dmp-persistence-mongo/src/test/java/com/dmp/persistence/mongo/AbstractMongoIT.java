package com.dmp.persistence.mongo;

import com.dmp.persistence.mongo.document.CheckpointDocument;
import com.dmp.persistence.mongo.document.RunDocument;
import com.dmp.persistence.mongo.document.SplitDocument;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base for MongoDB integration tests.
 *
 * <p>{@link MongoDBContainer} starts a single-node replica set, which is what the platform requires
 * anyway — change streams need an oplog (ADR-0002, ADR-0005). It is also what makes
 * {@code findAndModify} behave as it will in production; an embedded or standalone substitute would
 * pass these tests while the real deployment failed.
 */
@Testcontainers
@SpringBootTest(classes = MongoTestApplication.class)
public abstract class AbstractMongoIT {

    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8").withReuse(true);

    static {
        MONGO.start();
    }

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", MONGO::getReplicaSetUrl);
    }

    @Autowired
    protected MongoTemplate mongo;

    protected TenantId tenantId;

    @BeforeEach
    void resetCollections() {
        // Dropped rather than deleted so index state is rebuilt from scratch too, and one test's
        // leftover documents cannot make another test's count assertions pass for the wrong reason.
        mongo.dropCollection(RunDocument.class);
        mongo.dropCollection(SplitDocument.class);
        mongo.dropCollection(CheckpointDocument.class);
        tenantId = TenantId.newId();
    }
}
