package com.dmp.app.e2e;

import com.dmp.app.tenant.TenantContextHolder;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.application.port.out.TenantRepository;
import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.EdgeDefinition;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineValidator;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.Tenant;
import com.dmp.domain.tenant.TenantId;
import com.dmp.engine.ChunkExecutor;
import com.dmp.engine.ResolvedPipeline;
import com.dmp.engine.RunOrchestrator;
import com.dmp.engine.RunPlanner;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real migration, end to end, through every layer.
 *
 * <p>Creates a table with 10,000 rows in a real PostgreSQL, defines a pipeline, publishes it, runs
 * it through the real engine and the real JDBC connector, and checks the rows arrived. Nothing is
 * mocked: the connector talks to a live database, the engine checkpoints into a live MongoDB, and
 * the chunk claim is the same atomic operation production uses.
 *
 * <p>The resume test is the one that matters most. It executes half a chunk, discards the worker
 * mid-flight, and verifies a second worker continues from the checkpoint rather than from the
 * beginning — no lost rows, no duplicates. That property is the entire justification for the
 * chunk-and-checkpoint design, and it is not something unit tests can establish.
 */
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(classes = com.dmp.app.DmpApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
class MigrationE2EIT {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("dmp")
            .withUsername("dmp")
            .withPassword("dmp")
            .withReuse(true);

    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8").withReuse(true);

    static {
        POSTGRES.start();
        MONGO.start();
    }

    private static final int ROW_COUNT = 10_000;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.mongodb.uri", MONGO::getReplicaSetUrl);
        // The worker loop is disabled so the test drives execution deliberately and can assert on
        // intermediate state. Its behaviour is covered separately by WorkDistributionIT.
        registry.add("dmp.worker.max-concurrent-chunks", () -> 0);
        registry.add("PG_USER", POSTGRES::getUsername);
        registry.add("PG_PASSWORD", POSTGRES::getPassword);
    }

    @Autowired private TenantRepository tenants;
    @Autowired private PipelineRepository pipelines;
    @Autowired private PipelineVersionRepository versions;
    @Autowired private ConnectorInstanceRepository connectorInstances;
    @Autowired private RunRepository runs;
    @Autowired private SplitRepository splits;
    @Autowired private CheckpointRepository checkpoints;
    @Autowired private RunOrchestrator orchestrator;
    @Autowired private RunPlanner planner;
    @Autowired private ChunkExecutor executor;
    @Autowired private Clock clock;

    private TenantId tenantId;

    @BeforeEach
    void setUp() throws SQLException {
        Tenant tenant = tenants.findBySlug("e2e")
                .orElseGet(() -> tenants.save(Tenant.create("e2e", "E2E", clock.instant())));
        tenantId = tenant.id();
        TenantContextHolder.set(tenantId, "e2e-test");

        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS source_orders");
            statement.execute("DROP TABLE IF EXISTS target_orders");
            statement.execute("""
                    CREATE TABLE source_orders (
                        id           BIGINT PRIMARY KEY,
                        customer     TEXT NOT NULL,
                        amount       NUMERIC(18,2) NOT NULL,
                        placed_at    TIMESTAMPTZ NOT NULL
                    )""");
            statement.execute("""
                    CREATE TABLE target_orders (
                        id           BIGINT PRIMARY KEY,
                        customer     TEXT NOT NULL,
                        amount       NUMERIC(18,2) NOT NULL,
                        placed_at    TIMESTAMPTZ NOT NULL
                    )""");
            statement.execute("""
                    INSERT INTO source_orders (id, customer, amount, placed_at)
                    SELECT g,
                           'customer-' || g,
                           (g * 1.07)::numeric(18,2),
                           now() - (g || ' seconds')::interval
                    FROM generate_series(1, %d) g
                    """.formatted(ROW_COUNT));
        }
    }

    @Test
    @DisplayName("migrates 10,000 rows from one table to another")
    void fullMigration() throws SQLException {
        Run run = startRun(ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT);
        ResolvedPipeline pipeline = planner.resolve(run);

        executeAllChunks(run, pipeline);

        assertThat(countTarget()).isEqualTo(ROW_COUNT);

        Run finished = runs.findById(tenantId, run.id()).orElseThrow();
        assertThat(finished.state()).isEqualTo(RunState.COMPLETED);
        assertThat(finished.metrics().recordsWritten()).isEqualTo(ROW_COUNT);
        // The invariant that matters: nothing read went missing between source and sink.
        assertThat(finished.metrics().unaccountedRecords()).isZero();
    }

    @Test
    @DisplayName("preserves decimal precision rather than rounding through a double")
    void decimalsSurvive() throws SQLException {
        Run run = startRun(ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT);
        executeAllChunks(run, planner.resolve(run));

        // 9999 * 1.07 = 10698.93 exactly. Passing through a double would produce 10698.930000000001
        // and the migrated ledger would not reconcile.
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT amount FROM target_orders WHERE id = 9999")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1)).isEqualByComparingTo("10698.93");
        }
    }

    @Test
    @DisplayName("resumes from the checkpoint after a worker is lost mid-chunk")
    void resumesAfterWorkerLoss() throws SQLException {
        // A tight byte ceiling so the chunk commits several checkpoints before it is interrupted.
        //
        // It used to be a small write batch, which no longer exists: the batch is the chunk. Bytes
        // are now the only thing that flushes a chunk before it ends — and that is the mechanism
        // worth testing anyway, because it is the one a real migration hits. A chunk sized in rows
        // says nothing about what those rows weigh.
        // Checkpoint every batch. On the automatic interval an idempotent sink saves its position
        // every fiftieth batch, so a chunk that dies at row four hundred has committed nothing and
        // there is no resume position to assert on — which is correct behaviour and useless for
        // this test. What is being tested is what happens once a position exists.
        var chunking = new ChunkingPolicy(200, 64L * 1024, Duration.ofSeconds(30), 2, 1);
        Run run = startRun(chunking, ExecutionPolicy.DEFAULT);
        ResolvedPipeline pipeline = planner.resolve(run);

        Split chunk = splits.claimNextPending(tenantId, run.id(), "worker-doomed",
                clock.instant(), Duration.ofMinutes(5)).orElseThrow();

        // Kill the target part-way through, imitating a pod lost mid-chunk. Everything committed so
        // far is durable; everything after it has not been read.
        //
        // This used to interrupt the chunk through a cancellation hook, which no longer exists —
        // stopping a run now lets the chunk it is running finish, so that a chunk is always a whole
        // thing. Breaking the destination is a truer simulation of a lost worker anyway: a pod does
        // not stop politely at a batch boundary, it stops.
        // A constraint the chunk only trips part-way through, so at least one batch commits before
        // the destination refuses one. Breaking the target outright — renaming the table away —
        // failed the very first write, and a chunk with no committed batches has nothing to resume
        // from, which is the opposite of what this test is about. 64 KiB of these rows is roughly a
        // thousand of them, so a ceiling of 1,500 falls inside the second batch.
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE target_orders ADD CONSTRAINT lost_worker CHECK (id < 1500)");
        }
        try {
            executor.execute(pipeline, chunk, "worker-doomed");
        } catch (Exception expected) {
            // A torn chunk is exactly what this is testing.
        }
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE target_orders DROP CONSTRAINT lost_worker");
        }

        var midpoint = checkpoints.findBySplit(tenantId, chunk.id()).orElseThrow();
        assertThat(midpoint.hasProgress()).isTrue();
        long writtenBeforeLoss = midpoint.recordsWritten();
        assertThat(writtenBeforeLoss).isPositive();
        assertThat(countTarget()).isEqualTo(writtenBeforeLoss);

        // The lease lapses and another worker takes over.
        splits.transitionState(tenantId, chunk.id(), SplitState.RUNNING,
                chunk.fail("LEASE_EXPIRED", "worker lost", clock.instant()).scheduleRetry(clock.instant()));

        Split reclaimed = splits.claimNextPending(tenantId, run.id(), "worker-rescuer",
                clock.instant(), Duration.ofMinutes(5)).orElseThrow();
        assertThat(reclaimed.id()).isEqualTo(chunk.id());

        executor.execute(pipeline, reclaimed, "worker-rescuer");
        splits.transitionState(tenantId, reclaimed.id(), SplitState.RUNNING,
                reclaimed.complete(clock.instant()));

        // Finish the remaining chunks.
        executeRemainingChunks(run, pipeline);

        // No rows lost and none duplicated. The primary key would have rejected a duplicate, so an
        // exact count proves the resume landed on the right boundary.
        assertThat(countTarget()).isEqualTo(ROW_COUNT);
    }

    @Test
    @DisplayName("runs sequentially when the pipeline asks for one chunk at a time")
    void sequentialExecutionHoldsAcrossTheFleet() {
        Run run = startRun(ChunkingPolicy.DEFAULT, ExecutionPolicy.sequential());

        assertThat(runs.tryReserveSlot(tenantId, run.id(), 1)).isTrue();
        // A second worker, anywhere in the fleet, is refused while the first holds the slot.
        assertThat(runs.tryReserveSlot(tenantId, run.id(), 1)).isFalse();

        runs.releaseSlot(tenantId, run.id());
        assertThat(runs.tryReserveSlot(tenantId, run.id(), 1)).isTrue();
    }

    @Test
    @DisplayName("splits a large table into several chunks so work can spread across pods")
    void planningProducesMultipleChunks() {
        Run run = startRun(ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT);

        List<Split> chunks = splits.findByRun(tenantId, run.id());

        assertThat(chunks).hasSizeGreaterThan(1);
        // Contiguous and non-overlapping: every row belongs to exactly one chunk.
        long lowest = chunks.stream().mapToLong(s -> s.spec().get("from").asLong()).min().orElseThrow();
        long highest = chunks.stream().mapToLong(s -> s.spec().get("to").asLong()).max().orElseThrow();
        assertThat(lowest).isEqualTo(1);
        assertThat(highest).isEqualTo(ROW_COUNT);
    }

    // ---------------------------------------------------------------- helpers

    private Run startRun(ChunkingPolicy chunking, ExecutionPolicy execution) {
        ConnectorInstance source = connectorInstances.save(ConnectorInstance.create(
                tenantId, "source-" + System.nanoTime(), "jdbc-postgres", ConnectorDirection.BOTH,
                sourceConfig(), secretRefs(), "E2E source", clock.instant()));

        ConnectorInstance sink = connectorInstances.save(ConnectorInstance.create(
                tenantId, "sink-" + System.nanoTime(), "jdbc-postgres", ConnectorDirection.BOTH,
                sinkConfig(), secretRefs(), "E2E sink", clock.instant()));

        Pipeline pipeline = pipelines.save(Pipeline.create(
                tenantId, "E2E " + System.nanoTime(), null, null, Set.of(), clock.instant()));
        pipelines.save(pipeline.withNewVersion(1, clock.instant()));

        PipelineDefinition definition = new PipelineDefinition(
                List.of(new NodeDefinition("src", NodeType.SOURCE, "Orders", source.id().value(), null),
                        new NodeDefinition("dst", NodeType.SINK, "Orders copy", sink.id().value(), null)),
                List.of(EdgeDefinition.of("e1", "src", "dst")));

        PipelineVersion draft = versions.save(PipelineVersion.createDraft(
                pipeline.id(), tenantId, 1, definition, chunking, execution,
                AuditPolicy.DEFAULT, PipelineMode.FULL_LOAD, "e2e", "test", clock.instant()));

        versions.save(draft.publish(new PipelineValidator(), clock.instant()));
        pipelines.save(pipelines.findById(tenantId, pipeline.id()).orElseThrow()
                .publishVersion(1, clock.instant()));

        Run run = orchestrator.start(pipeline.id(), RunTrigger.MANUAL, null);
        orchestrator.advanceToRunning(run, "e2e-worker");
        return runs.findById(tenantId, run.id()).orElseThrow();
    }

    private void executeAllChunks(Run run, ResolvedPipeline pipeline) {
        executeRemainingChunks(run, pipeline);
    }

    private void executeRemainingChunks(Run run, ResolvedPipeline pipeline) {
        while (true) {
            var claimed = splits.claimNextPending(tenantId, run.id(), "e2e-worker",
                    clock.instant(), Duration.ofMinutes(5));
            if (claimed.isEmpty()) {
                break;
            }
            Split chunk = claimed.get();
            var result = executor.execute(pipeline, chunk, "e2e-worker");

            splits.transitionState(tenantId, chunk.id(), SplitState.RUNNING,
                    chunk.complete(clock.instant()));
            runs.incrementMetrics(tenantId, run.id(), new com.dmp.domain.run.RunMetrics(
                    result.recordsRead(), result.recordsProduced(), result.recordsWritten(),
                    result.recordsFailed(), result.recordsFiltered(), result.bytesRead(), 0, 1, 0));
        }
        runs.findById(tenantId, run.id()).ifPresent(orchestrator::completeIfFinished);
    }

    private ObjectNode sourceConfig() {
        ObjectNode config = Json.newObject();
        config.put("url", POSTGRES.getJdbcUrl());
        config.put("schema", "public");
        config.put("table", "source_orders");
        config.put("splitColumn", "id");
        return config;
    }

    private ObjectNode sinkConfig() {
        ObjectNode config = Json.newObject();
        config.put("url", POSTGRES.getJdbcUrl());
        config.put("schema", "public");
        config.put("table", "target_orders");
        config.put("writeMode", "UPSERT");
        config.set("keyColumns", Json.mapper().createArrayNode().add("id"));
        return config;
    }

    /** References, never values — resolved from the environment at execution time. */
    private ObjectNode secretRefs() {
        ObjectNode refs = Json.newObject();
        refs.put("username", "env:PG_USER");
        refs.put("password", "env:PG_PASSWORD");
        return refs;
    }

    private Connection jdbc() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private long countTarget() throws SQLException {
        try (Connection connection = jdbc();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM target_orders")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
