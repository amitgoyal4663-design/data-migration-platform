package com.dmp.persistence.mongo;

import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.run.SplitState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tests that matter most in Phase 1.
 *
 * <p>Work distribution rests entirely on two atomic MongoDB operations behaving correctly under
 * genuine concurrency: the claim, and the slot reservation. Everything else in the engine assumes
 * they hold. Asserting them against a real replica set with real racing threads is the only way to
 * know they do — a mocked repository would agree with whatever the implementation happened to do.
 */
class WorkDistributionIT extends AbstractMongoIT {

    private static final Instant NOW = Instant.parse("2026-08-07T00:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(5);

    @Autowired
    private RunRepository runs;

    @Autowired
    private SplitRepository splits;

    private Run newRun() {
        return runs.create(Run.create(tenantId, PipelineId.newId(), PipelineVersionId.newId(), 1,
                PipelineMode.FULL_LOAD, RunTrigger.MANUAL, null, "tester", NOW));
    }

    private void planChunks(Run run, int count) {
        List<Split> planned = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            planned.add(Split.plan(run.id(), tenantId, i,
                    Json.newObject().put("from", i * 1000).put("to", (i + 1) * 1000), NOW));
        }
        splits.saveAll(planned);
    }

    /** Runs every task simultaneously, releasing them from a latch so they genuinely race. */
    private <T> List<T> race(int threads, Callable<T> task) throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return task.call();
                }));
            }
            start.countDown();

            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
            return results;
        }
    }

    @Test
    @DisplayName("twenty workers racing for ten chunks each get a different one")
    void concurrentClaimsNeverCollide() throws Exception {
        Run run = newRun();
        planChunks(run, 10);

        List<Optional<Split>> claims = race(20, () ->
                splits.claimNextPending(tenantId, run.id(), "worker-" + Thread.currentThread().threadId(),
                        NOW, LEASE));

        List<Split> won = claims.stream().flatMap(Optional::stream).toList();
        Set<SplitId> distinct = new HashSet<>(won.stream().map(Split::id).toList());

        // Exactly ten claims succeed, and no two workers received the same chunk. If this ever
        // fails, every record in the overlapping chunk is written twice.
        assertThat(won).hasSize(10);
        assertThat(distinct).hasSize(10);
        assertThat(splits.countByState(tenantId, run.id(), SplitState.PENDING)).isZero();
    }

    @Test
    @DisplayName("claims chunks in index order so progress is legible")
    void claimsInIndexOrder() {
        Run run = newRun();
        planChunks(run, 5);

        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            splits.claimNextPending(tenantId, run.id(), "worker", NOW, LEASE)
                    .ifPresent(split -> order.add(split.index()));
        }

        assertThat(order).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("returns empty rather than blocking when nothing is pending")
    void emptyWhenNoWork() {
        Run run = newRun();

        assertThat(splits.claimNextPending(tenantId, run.id(), "worker", NOW, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("with a limit of one, exactly one of fifty racing workers reserves the slot")
    void sequentialAllowsExactlyOneWorker() throws Exception {
        Run run = newRun();
        AtomicInteger winners = new AtomicInteger();

        race(50, () -> {
            if (runs.tryReserveSlot(tenantId, run.id(), 1)) {
                winners.incrementAndGet();
            }
            return null;
        });

        // This is the whole guarantee behind sequential execution. One winner across fifty
        // simultaneous attempts, with no lock and no coordinator.
        assertThat(winners.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("with a limit of four, exactly four of fifty racing workers reserve a slot")
    void limitedConcurrencyIsExact() throws Exception {
        Run run = newRun();
        AtomicInteger winners = new AtomicInteger();

        race(50, () -> {
            if (runs.tryReserveSlot(tenantId, run.id(), 4)) {
                winners.incrementAndGet();
            }
            return null;
        });

        assertThat(winners.get()).isEqualTo(4);
        assertThat(runs.findById(tenantId, run.id()).orElseThrow().activeSlots()).isEqualTo(4);
    }

    @Test
    @DisplayName("skips reservation entirely when unlimited")
    void unlimitedAlwaysSucceeds() throws Exception {
        Run run = newRun();

        List<Boolean> results = race(20, () ->
                runs.tryReserveSlot(tenantId, run.id(), ExecutionPolicy.UNLIMITED));

        assertThat(results).containsOnly(true);
        // No document write occurs, so the counter stays at zero rather than tracking work the
        // platform is not limiting.
        assertThat(runs.findById(tenantId, run.id()).orElseThrow().activeSlots()).isZero();
    }

    @Test
    @DisplayName("frees the slot on release so the next worker can take it")
    void releaseFreesTheSlot() {
        Run run = newRun();

        assertThat(runs.tryReserveSlot(tenantId, run.id(), 1)).isTrue();
        assertThat(runs.tryReserveSlot(tenantId, run.id(), 1)).isFalse();

        runs.releaseSlot(tenantId, run.id());

        assertThat(runs.tryReserveSlot(tenantId, run.id(), 1)).isTrue();
    }

    @Test
    @DisplayName("never lets the slot counter go negative on a double release")
    void doubleReleaseIsSafe() {
        Run run = newRun();
        runs.tryReserveSlot(tenantId, run.id(), 2);

        runs.releaseSlot(tenantId, run.id());
        runs.releaseSlot(tenantId, run.id());
        runs.releaseSlot(tenantId, run.id());

        // A negative counter would let the run permanently exceed its limit — on a sequential
        // pipeline, exactly the concurrent writes the setting existed to prevent.
        assertThat(runs.findById(tenantId, run.id()).orElseThrow().activeSlots()).isZero();
    }

    @Test
    @DisplayName("recovers from a leaked slot through reconciliation")
    void reconciliationClearsDrift() {
        Run run = newRun();
        planChunks(run, 3);

        // Simulate three workers that died holding slots without releasing them.
        runs.tryReserveSlot(tenantId, run.id(), 10);
        runs.tryReserveSlot(tenantId, run.id(), 10);
        runs.tryReserveSlot(tenantId, run.id(), 10);
        assertThat(runs.findById(tenantId, run.id()).orElseThrow().activeSlots()).isEqualTo(3);

        int actuallyRunning = (int) splits.countRunning(tenantId, run.id());
        runs.reconcileSlots(tenantId, run.id(), actuallyRunning);

        // Without this, a sequential run whose worker died would deadlock forever: the counter
        // says one is running, nothing is, and no worker can ever reserve again.
        assertThat(runs.findById(tenantId, run.id()).orElseThrow().activeSlots()).isZero();
    }

    @Test
    @DisplayName("finds a chunk whose lease has lapsed")
    void expiredLeaseIsDetected() {
        Run run = newRun();
        planChunks(run, 1);

        splits.claimNextPending(tenantId, run.id(), "doomed-worker", NOW, Duration.ofSeconds(30));

        Instant afterExpiry = NOW.plus(Duration.ofMinutes(1));

        assertThat(splits.findExpiredLeases(NOW, 10)).isEmpty();
        assertThat(splits.findExpiredLeases(afterExpiry, 10)).hasSize(1);
    }

    @Test
    @DisplayName("extends the lease while the worker keeps reporting")
    void heartbeatExtendsTheLease() {
        Run run = newRun();
        planChunks(run, 1);

        Split claimed = splits.claimNextPending(tenantId, run.id(), "worker-a", NOW,
                Duration.ofSeconds(30)).orElseThrow();

        Instant later = NOW.plus(Duration.ofSeconds(20));
        splits.heartbeat(tenantId, claimed.id(), "worker-a", later, Duration.ofSeconds(30));

        // The original lease would have lapsed by now; the heartbeat pushed it out.
        assertThat(splits.findExpiredLeases(NOW.plus(Duration.ofSeconds(35)), 10)).isEmpty();
    }

    @Test
    @DisplayName("refuses a heartbeat from a worker that no longer holds the chunk")
    void zombieWorkerCannotReclaimItsLease() {
        Run run = newRun();
        planChunks(run, 1);

        Split claimed = splits.claimNextPending(tenantId, run.id(), "worker-a", NOW, LEASE).orElseThrow();

        // The sweep reclaims it and a second worker takes over.
        splits.transitionState(tenantId, claimed.id(), SplitState.RUNNING,
                claimed.fail("LEASE_EXPIRED", "reclaimed", NOW).scheduleRetry(NOW));
        Split reclaimed = splits.claimNextPending(tenantId, run.id(), "worker-b", NOW, LEASE).orElseThrow();

        // Worker A wakes up and tries to carry on. It must be refused — otherwise both workers
        // would believe they own the chunk and write every one of its records twice.
        assertThat(splits.heartbeat(tenantId, claimed.id(), "worker-a", NOW, LEASE)).isEmpty();
        assertThat(splits.heartbeat(tenantId, reclaimed.id(), "worker-b", NOW, LEASE)).isPresent();
    }

    @Test
    @DisplayName("rejects a duplicate idempotency key so a redelivered trigger cannot start twice")
    void idempotencyKeyIsEnforced() throws Exception {
        // Ensures the partial unique index exists before the race, since index creation is
        // explicit rather than automatic.
        new com.dmp.persistence.mongo.config.MongoIndexInitializer(mongo).createIndexes();

        String key = "schedule-2026-08-07T03:00Z";
        PipelineId pipelineId = PipelineId.newId();
        PipelineVersionId versionId = PipelineVersionId.newId();

        AtomicInteger created = new AtomicInteger();
        race(10, () -> {
            try {
                runs.create(Run.create(tenantId, pipelineId, versionId, 1, PipelineMode.FULL_LOAD,
                        RunTrigger.SCHEDULED, key, "scheduler", NOW));
                created.incrementAndGet();
            } catch (Exception expected) {
                // The delay queue is at-least-once by design (ADR-0002), so a duplicate attempt is
                // an ordinary outcome rather than an error.
            }
            return null;
        });

        assertThat(created.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("searches without a sort parameter, which is what the console sends by default")
    void searchWithoutSortDoesNotFail() {
        // Regression: Map.of() throws on getOrDefault(null, ...) rather than returning the
        // default, so an unsorted search failed with a NullPointerException — on the endpoint
        // the console polls every two seconds.
        newRun();

        var result = runs.search(tenantId, RunRepository.RunSearch.none(),
                new com.dmp.application.common.PageQuery(0, 25, null, false));

        assertThat(result.content()).hasSize(1);
    }

    @Test
    @DisplayName("falls back to the default sort when asked for an unknown field")
    void unknownSortFallsBack() {
        newRun();

        var result = runs.search(tenantId, RunRepository.RunSearch.none(),
                new com.dmp.application.common.PageQuery(0, 25, "notAField", false));

        assertThat(result.content()).hasSize(1);
    }

    @Test
    @DisplayName("allows many runs with no idempotency key, thanks to the partial index")
    void nullIdempotencyKeysDoNotCollide() {
        new com.dmp.persistence.mongo.config.MongoIndexInitializer(mongo).createIndexes();

        // A plain unique index would permit exactly one keyless run per tenant, ever.
        newRun();
        newRun();
        newRun();

        assertThat(runs.findActive(tenantId)).isNotNull();
    }
}
