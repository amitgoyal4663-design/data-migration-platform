package com.dmp.connector.databricks;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local stand-in for the Databricks SQL Statement Execution API.
 *
 * <p><b>What this proves and what it does not.</b> It proves the connector drives the statement
 * lifecycle correctly: that it submits once and polls rather than blocking, that it waits for
 * SUCCEEDED rather than treating PENDING as an answer, that it plans from the manifest's own chunk
 * boundaries, that it resolves an external link at read time rather than at planning time, and that
 * a resumed read skips exactly what was already handed to the sink. Those are the things that break.
 *
 * <p>It cannot prove that this is what Databricks actually does. The fake answers whatever it is
 * told to answer, so a misreading of the real API would be faithfully reproduced here and pass.
 * Only a workspace can close that gap, and this exists so that when one is available it is a
 * confirmation rather than the first test.
 *
 * <p>Every request is recorded, because several of the interesting assertions are about the
 * <em>sequence</em> and about what was <em>not</em> called — "the link was fetched after planning",
 * "no Authorization header reached cloud storage" — which are only visible in the trace.
 */
final class FakeDatabricks implements AutoCloseable {

    private final HttpServer server;
    private final List<String> calls = new ArrayList<>();
    private final List<String> externalAuthHeaders = new ArrayList<>();
    private final AtomicInteger statementCounter = new AtomicInteger();
    private final Map<String, AtomicInteger> polls = new java.util.HashMap<>();

    /** Columns of the result, as {@code name:TYPE_NAME}. */
    private List<String> columns = List.of("id:LONG", "name:STRING");

    /** The result, one inner list per result chunk, each row a list of stringified values. */
    private List<List<List<String>>> chunks = List.of();

    /** How many times a statement is reported PENDING before it succeeds. */
    private int pollsBeforeSuccess = 1;

    /** When set, the statement reaches this terminal state instead of SUCCEEDED. */
    private String failWith;
    private String failMessage = "";

    /** Whether chunks are served inline or as links back into this server. */
    private boolean externalLinks;

    private boolean cancelled;

    FakeDatabricks() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::route);
        server.setExecutor(null);
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    List<String> calls() {
        return List.copyOf(calls);
    }

    List<String> externalAuthHeaders() {
        return List.copyOf(externalAuthHeaders);
    }

    boolean wasCancelled() {
        return cancelled;
    }

    FakeDatabricks columns(String... nameAndType) {
        this.columns = List.of(nameAndType);
        return this;
    }

    FakeDatabricks chunks(List<List<List<String>>> chunks) {
        this.chunks = chunks;
        return this;
    }

    FakeDatabricks pollsBeforeSuccess(int polls) {
        this.pollsBeforeSuccess = polls;
        return this;
    }

    FakeDatabricks failWith(String state, String message) {
        this.failWith = state;
        this.failMessage = message;
        return this;
    }

    FakeDatabricks externalLinks() {
        this.externalLinks = true;
        return this;
    }

    private void route(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        calls.add(method + " " + path);

        try {
            if (path.endsWith("/oidc/v1/token")) {
                respond(exchange, 200, "{\"access_token\":\"fake-oauth-token\",\"expires_in\":3600}");

            } else if (method.equals("GET") && path.contains("/sql/warehouses/")) {
                respond(exchange, 200, "{\"id\":\"" + segment(path, 1)
                        + "\",\"name\":\"Fake Warehouse\",\"state\":\"RUNNING\"}");

            } else if (method.equals("POST") && path.endsWith("/cancel")) {
                cancelled = true;
                respond(exchange, 200, "{}");

            } else if (method.equals("POST") && path.endsWith("/sql/statements")) {
                String id = "stmt-" + statementCounter.incrementAndGet();
                respond(exchange, 200,
                        "{\"statement_id\":\"" + id + "\",\"status\":{\"state\":\"PENDING\"}}");

            } else if (method.equals("GET") && path.contains("/result/chunks/")) {
                serveChunk(exchange, Integer.parseInt(segment(path, 1)), segment(path, 4));

            } else if (method.equals("GET") && path.startsWith("/external/")) {
                // Recorded so a test can assert the workspace credential was NOT sent to storage.
                externalAuthHeaders.add(String.valueOf(
                        exchange.getRequestHeaders().getFirst("Authorization")));
                respond(exchange, 200, rowsJson(chunks.get(Integer.parseInt(segment(path, 1)))));

            } else if (method.equals("GET") && path.contains("/sql/statements/")) {
                serveStatus(exchange, segment(path, 1));

            } else {
                respond(exchange, 404, "{\"message\":\"no route for " + method + " " + path + "\"}");
            }
        } catch (RuntimeException e) {
            respond(exchange, 500, "{\"message\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * Answers PENDING a configured number of times, then the terminal state.
     *
     * <p>The manifest arrives only with SUCCEEDED, exactly as it does in the real API — so a
     * connector that planned before the statement finished would find nothing to plan from.
     */
    private void serveStatus(HttpExchange exchange, String statementId) throws IOException {
        int seen = polls.computeIfAbsent(statementId, k -> new AtomicInteger()).getAndIncrement();

        if (seen < pollsBeforeSuccess) {
            respond(exchange, 200, "{\"statement_id\":\"" + statementId
                    + "\",\"status\":{\"state\":\"PENDING\"}}");
            return;
        }
        if (failWith != null) {
            respond(exchange, 200, "{\"statement_id\":\"" + statementId
                    + "\",\"status\":{\"state\":\"" + failWith + "\",\"error\":{"
                    + "\"error_code\":\"BAD_REQUEST\",\"message\":\"" + failMessage + "\"}}}");
            return;
        }
        respond(exchange, 200, "{\"statement_id\":\"" + statementId
                + "\",\"status\":{\"state\":\"SUCCEEDED\"},\"manifest\":" + manifest() + "}");
    }

    private void serveChunk(HttpExchange exchange, int index, String statementId) throws IOException {
        List<List<String>> rows = index < chunks.size() ? chunks.get(index) : List.of();

        if (externalLinks) {
            respond(exchange, 200, "{\"external_links\":[{\"chunk_index\":" + index
                    + ",\"row_count\":" + rows.size()
                    + ",\"external_link\":\"" + url() + "/external/" + index + "\"}]}");
            return;
        }
        respond(exchange, 200, "{\"chunk_index\":" + index + ",\"row_count\":" + rows.size()
                + ",\"data_array\":" + rowsJson(rows) + "}");
    }

    private String manifest() {
        StringBuilder columnJson = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            String[] parts = columns.get(i).split(":");
            if (!columnJson.isEmpty()) {
                columnJson.append(',');
            }
            columnJson.append("{\"name\":\"").append(parts[0])
                    .append("\",\"type_name\":\"").append(parts[1])
                    .append("\",\"position\":").append(i).append('}');
        }

        StringBuilder chunkJson = new StringBuilder();
        long offset = 0;
        long total = 0;
        for (int i = 0; i < chunks.size(); i++) {
            int rows = chunks.get(i).size();
            if (!chunkJson.isEmpty()) {
                chunkJson.append(',');
            }
            chunkJson.append("{\"chunk_index\":").append(i)
                    .append(",\"row_offset\":").append(offset)
                    .append(",\"row_count\":").append(rows).append('}');
            offset += rows;
            total += rows;
        }

        return "{\"format\":\"JSON_ARRAY\",\"schema\":{\"column_count\":" + columns.size()
                + ",\"columns\":[" + columnJson + "]},\"total_chunk_count\":" + chunks.size()
                + ",\"total_row_count\":" + total + ",\"chunks\":[" + chunkJson + "]}";
    }

    /** JSON_ARRAY: every value is a string or null, whatever the declared column type says. */
    private static String rowsJson(List<List<String>> rows) {
        StringBuilder json = new StringBuilder("[");
        for (List<String> row : rows) {
            if (json.length() > 1) {
                json.append(',');
            }
            json.append('[');
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    json.append(',');
                }
                String value = row.get(i);
                json.append(value == null ? "null" : "\"" + value + "\"");
            }
            json.append(']');
        }
        return json.append(']').toString();
    }

    private static String segment(String path, int fromEnd) {
        String[] parts = path.split("/");
        return parts[parts.length - fromEnd];
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
