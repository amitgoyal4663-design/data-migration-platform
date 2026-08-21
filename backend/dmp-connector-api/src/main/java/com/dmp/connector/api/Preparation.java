package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;

/**
 * A handle on an asynchronous external job.
 *
 * <p>Some systems cannot be read or written immediately. Salesforce Bulk API v2, Databricks SQL
 * statements, Athena queries and BigQuery extracts all follow the same shape: submit a job, poll
 * until it finishes, then fetch results, then release it. A connector for one of these returns a
 * handle here and the engine polls it, rather than blocking a thread for what may be hours.
 *
 * <p>The handle is <b>persisted on the run</b>, so the worker that submitted the job need not be
 * the one that observes it finishing — or releases it. A connector must therefore put everything
 * it needs to resume into {@code state}, and hold nothing in instance fields.
 *
 * <p>Connectors needing no preparation return {@link #none()}, which is immediately ready. The
 * common case costs one method call and no state.
 *
 * @param state connector-defined; a Salesforce job id, a Databricks statement id, a query token
 */
public record Preparation(JsonNode state) {

    public Preparation {
        state = Json.orEmpty(state);
    }

    /** For connectors that can be read or written immediately. */
    public static Preparation none() {
        return new Preparation(Json.emptyObject());
    }

    public static Preparation of(JsonNode state) {
        return new Preparation(state);
    }

    public boolean isEmpty() {
        return state.isEmpty();
    }

    /** Where a connector's preparation stands, and when to ask again. */
    public record Status(State state, String message, Duration retryAfter) {

        public static Status ready() {
            return new Status(State.READY, null, Duration.ZERO);
        }

        /**
         * Still working.
         *
         * <p>The connector chooses the interval, because only it knows the shape of the system it
         * is waiting on. Polling a Salesforce bulk job every second consumes API quota for no
         * benefit; polling a two-second Databricks statement every minute wastes most of a minute.
         */
        public static Status pending(Duration retryAfter) {
            return new Status(State.PENDING, null, retryAfter);
        }

        public static Status failed(String message) {
            return new Status(State.FAILED, message, Duration.ZERO);
        }

        public boolean isReady() {
            return state == State.READY;
        }

        public boolean isFailed() {
            return state == State.FAILED;
        }

        public enum State {
            PENDING,
            READY,
            FAILED
        }
    }
}
