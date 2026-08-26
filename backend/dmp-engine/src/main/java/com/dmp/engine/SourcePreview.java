package com.dmp.engine;

import com.dmp.application.service.ConnectorInstanceService;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.QueryVariants;
import com.dmp.connector.api.Source;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A handful of rows from a source, before anybody has built a pipeline on it.
 *
 * <p>Answers the question every mapping starts with and which the platform previously had no way
 * to answer: <em>what does a record from this system actually look like?</em> Until this existed
 * the field names went into a script from memory or from a screenshot, and the first thing that
 * ever compared them with reality was a production run — which is where a numeric field typed as a
 * string, or a nested object assumed to be flat, was discovered.
 *
 * <p>Deliberately not a run. Nothing is planned, no chunk exists, no checkpoint is written and
 * nothing is recorded against a pipeline. This opens a session, reads a few rows and closes it.
 * A preview appearing in the run history as a migration that moved eleven records would be worse
 * than no preview.
 */
@Service
public class SourcePreview {

    private static final Logger log = LoggerFactory.getLogger(SourcePreview.class);

    /**
     * Ceiling on rows, whatever is asked for.
     *
     * <p>A preview is read to learn the shape of a record, and the shape is evident from a handful.
     * The cap matters because the payloads travel to a browser and are held in memory here first —
     * and because "preview" invites somebody to try ten thousand and use it as an export.
     */
    private static final int MAX_ROWS = 100;
    private static final int DEFAULT_ROWS = 10;

    /**
     * How long to wait for a source that has to be asked twice.
     *
     * <p>A warehouse submits a statement and answers PENDING until it is ready. That is normal and
     * the wait is usually seconds, but it is somebody sitting in front of a form — so this gives up
     * and says why rather than holding a request open indefinitely. Short enough that a slow
     * warehouse produces an honest "not ready yet, try again" instead of a timeout somewhere else
     * in the stack that says nothing.
     */
    private static final Duration PREPARATION_BUDGET = Duration.ofSeconds(20);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private final ConnectorInstanceService instances;
    private final ConnectorRegistry connectors;
    private final ConnectorContexts contexts;

    public SourcePreview(ConnectorInstanceService instances, ConnectorRegistry connectors,
                         ConnectorContexts contexts) {
        this.instances = instances;
        this.connectors = connectors;
        this.contexts = contexts;
    }

    /**
     * The placeholders this source's query expects, if any.
     *
     * <p>Asked before reading, so a preview of a parameterised source can offer the right boxes
     * instead of failing with the connector's refusal. Costs no connection and resolves no
     * credential — the connector reads them out of its stored configuration.
     */
    public java.util.Set<String> parameterNames(ConnectorInstanceId id) {
        return parameterNames(id, null);
    }

    /**
     * The placeholders one of this connection's named queries expects.
     *
     * <p>Per query, for the same reason the run dialog asks per query: "by date range" wants a from
     * and a to, "by policy number" wants a list, and a form asking for all three would be asking for
     * values two of which cannot be used.
     */
    public java.util.Set<String> parameterNames(ConnectorInstanceId id, String query) {
        ConnectorInstance instance = instances.get(id);
        if (!connectors.require(instance.connectorType()).spec().direction().canRead()) {
            return java.util.Set.of();
        }
        return connectors.source(instance.connectorType())
                .parameterNames(QueryVariants.apply(instance.config(), resolve(instance, query)));
    }

    /** Which of those take a list, so the dialog offers a list rather than a single box. */
    public java.util.Set<String> listParameterNames(ConnectorInstanceId id, String query) {
        ConnectorInstance instance = instances.get(id);
        if (!connectors.require(instance.connectorType()).spec().direction().canRead()) {
            return java.util.Set.of();
        }
        return connectors.source(instance.connectorType())
                .listParameterNames(QueryVariants.apply(instance.config(), resolve(instance, query)));
    }

    /** The named queries this connection offers, in the order they were written. */
    public java.util.List<String> queryNames(ConnectorInstanceId id) {
        return QueryVariants.names(instances.get(id).config());
    }

    /**
     * The query a preview should run when none was named.
     *
     * <p>The first declared, which is what a run gets. A preview whose default differed from a
     * run's would be the least useful thing this could be: a sample of records the pipeline will
     * never read, shown to somebody about to write a mapping from it.
     */
    private static String resolve(ConnectorInstance instance, String query) {
        return query == null || query.isBlank()
                ? QueryVariants.defaultName(instance.config())
                : query;
    }

    /**
     * Reads up to {@code limit} records.
     *
     * @param parameters values for a parameterised query, exactly as a run would supply them —
     *                   a source whose SQL reads {@code WHERE ts > :from} cannot be previewed
     *                   without them, and guessing a value here would preview a different query
     *                   from the one the pipeline will run
     */
    public Result read(ConnectorInstanceId id, int limit, JsonNode parameters) {
        return read(id, limit, parameters, null);
    }

    /**
     * Reads up to {@code limit} records using one of the connection's named queries.
     *
     * <p>The whole point of a preview is that it shows what a run would read. It did not: the
     * connector was handed the instance's raw configuration, so a connection whose selection lives
     * in named queries had no selection at all here, and the preview returned the first ten rows of
     * the entire collection — records no run would ever touch, shown to somebody about to write a
     * mapping from them.
     */
    public Result read(ConnectorInstanceId id, int limit, JsonNode parameters, String query) {
        ConnectorInstance instance = instances.get(id);
        int rows = Math.clamp(limit <= 0 ? DEFAULT_ROWS : limit, 1, MAX_ROWS);

        if (!connectors.require(instance.connectorType()).spec().direction().canRead()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "'" + instance.name() + "' is a destination. There is nothing to preview.",
                    Map.of("connectorInstanceId", id.toString()));
        }

        Source source = connectors.source(instance.connectorType());
        ConnectorContext context = contexts.forInstance(
                instance, "preview", "console", parameters, resolve(instance, query));

        Instant startedAt = Instant.now();
        Preparation preparation = Preparation.none();

        try (Source.SourceSession session = source.openSource(context)) {
            preparation = session.prepare();
            preparation = awaitReady(session, preparation, instance.name());

            List<Source.SplitSpec> plan = session.plan(preparation,
                    new Source.PlanRequest(rows, 1));
            if (plan.isEmpty()) {
                return new Result(List.of(), null, elapsed(startedAt), false);
            }

            // The first chunk only. A preview of the first ten rows of the first chunk is what
            // somebody wants; walking the plan to fill a quota would read from several places in
            // the source to produce a sample that looks like one place.
            Source.SplitSpec first = plan.get(0);
            List<JsonNode> records = new ArrayList<>(rows);
            String describedRead;

            try (Source.RecordStream stream =
                         session.read(first, com.dmp.common.json.Json.emptyObject(), rows)) {
                describedRead = stream.describe();
                DataRecord record;
                while (records.size() < rows && (record = stream.next()) != null) {
                    records.add(record.payload());
                }
            }
            return new Result(List.copyOf(records), describedRead, elapsed(startedAt),
                    records.size() == rows);

        } catch (ConnectorException e) {
            // The connector's own words, unchanged. It knows that a table does not exist or that a
            // credential was refused; the platform would only be able to say the preview failed.
            throw new DmpException(ErrorCode.UPSTREAM_UNAVAILABLE, e.getMessage(),
                    Map.of("connectorInstanceId", id.toString(),
                            "connectorType", instance.connectorType()));
        } finally {
            release(source, context, preparation, instance);
        }
    }

    /**
     * Waits for a source that submits a job before it can be read.
     *
     * <p>Polls rather than blocking inside the connector, so the budget is enforced here and the
     * message names the connector that ran out of it.
     */
    private Preparation awaitReady(Source.SourceSession session, Preparation preparation,
                                   String name) {
        Instant deadline = Instant.now().plus(PREPARATION_BUDGET);

        while (true) {
            Preparation.Status status = session.checkPreparation(preparation);
            if (status.isReady()) {
                return preparation;
            }
            if (status.isFailed()) {
                throw new DmpException(ErrorCode.UPSTREAM_UNAVAILABLE,
                        "'" + name + "' could not prepare a preview: " + status.message(),
                        Map.of("connector", name));
            }
            if (Instant.now().isAfter(deadline)) {
                throw new DmpException(ErrorCode.UPSTREAM_UNAVAILABLE,
                        "'" + name + "' is still preparing after "
                                + PREPARATION_BUDGET.toSeconds() + " seconds. That is normal for a "
                                + "warehouse under load — try again in a moment. A run would wait; "
                                + "a preview will not hold the page open.",
                        Map.of("connector", name));
            }
            try {
                Thread.sleep(POLL_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DmpException(ErrorCode.INTERNAL, "Preview was interrupted");
            }
        }
    }

    /**
     * Always, and never allowed to mask the real failure.
     *
     * <p>A preview that submitted a Databricks statement must cancel it even when the read threw,
     * or a form somebody clicked twice leaves two statements running. A failure to release is
     * logged and swallowed: the caller's problem is whatever went wrong first.
     */
    private void release(Source source, ConnectorContext context, Preparation preparation,
                         ConnectorInstance instance) {
        if (preparation == null || preparation.isEmpty()) {
            return;
        }
        try (Source.SourceSession session = source.openSource(context)) {
            session.release(preparation);
        } catch (Exception e) {
            log.warn("Could not release the preview's resources on '{}': {}",
                    instance.name(), e.getMessage());
        }
    }

    private static long elapsed(Instant startedAt) {
        return Duration.between(startedAt, Instant.now()).toMillis();
    }

    /**
     * @param records what the source produced, unaltered — this is the shape somebody is here to
     *                see, and reformatting it would show them the platform's idea of their data
     * @param query   what was actually asked, in the source's own language, so a preview that
     *                returns nothing is diagnosable rather than merely disappointing
     * @param more    whether the source had more to give. A preview that stopped at the limit and
     *                one that reached the end of the data look identical without this
     */
    public record Result(List<JsonNode> records, String query, long durationMillis, boolean more) {
    }
}
