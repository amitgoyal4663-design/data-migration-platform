package com.dmp.connector.salesforce;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local stand-in for Salesforce's Bulk API 2.0.
 *
 * <p><b>What this proves and what it does not.</b> It proves the connector drives the job lifecycle
 * correctly: that it creates a job before writing, uploads once rather than per batch, closes the
 * job before polling, keeps polling while the org says InProgress, harvests per-record failures,
 * and pages query results by the locator it is given. Those are the things that break.
 *
 * <p>It cannot prove that this is what Salesforce actually does. The fake answers whatever it is
 * told to answer, so a misreading of the real API would be faithfully reproduced here and pass.
 * Only an org can close that gap, and this exists so that when one is available it is a
 * confirmation rather than the first test.
 *
 * <p>Every request is recorded, because the interesting assertions are about the <em>sequence</em>
 * of calls rather than the final state — "uploaded once" and "closed before polling" are only
 * visible in the order.
 */
final class FakeSalesforce implements AutoCloseable {

    static final String API = "v62.0";

    private final HttpServer server;
    private final List<String> calls = new ArrayList<>();
    private final Map<String, String> uploads = new HashMap<>();
    private final AtomicInteger jobCounter = new AtomicInteger();

    /** How many times a job is reported InProgress before it completes. */
    private int pollsBeforeComplete = 2;
    private final Map<String, AtomicInteger> polls = new HashMap<>();

    /** CSV returned by failedResults; empty means the job rejected nothing. */
    private String failedResults = "";

    /** Jobs the connector asked the org to delete, so a test can assert it asked for none. */
    private final java.util.List<String> deleted =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    /** Job ids this fake was told to delete. */
    public java.util.List<String> deletedJobs() {
        return java.util.List.copyOf(deleted);
    }

    /** The failure count this fake reports in a job status document. */
    public long reportedFailedCount() {
        return failedResults.isBlank() ? 0 : Math.max(0, failedResults.strip().lines().count() - 1);
    }

    /** Query result pages, served in order; the locator is the index of the next one. */
    private List<String> queryPages = List.of();

    FakeSalesforce() throws IOException {
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

    String uploadFor(String jobId) {
        return uploads.get(jobId);
    }

    FakeSalesforce pollsBeforeComplete(int polls) {
        this.pollsBeforeComplete = polls;
        return this;
    }

    FakeSalesforce failedResults(String csv) {
        this.failedResults = csv;
        return this;
    }

    FakeSalesforce queryPages(List<String> pages) {
        this.queryPages = pages;
        return this;
    }

    private void route(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getQuery();
        calls.add(method + " " + path);

        try {
            if (path.endsWith("/services/oauth2/token")) {
                respond(exchange, 200, """
                        {"access_token":"fake-token","instance_url":"%s","token_type":"Bearer"}
                        """.formatted(url()));

            } else if (method.equals("POST") && path.endsWith("/jobs/ingest")) {
                String id = "job-" + jobCounter.incrementAndGet();
                respond(exchange, 200, "{\"id\":\"" + id + "\",\"state\":\"Open\"}");

            } else if (method.equals("PUT") && path.endsWith("/batches")) {
                String jobId = segment(path, 2);
                // Recorded rather than discarded: "the CSV has one header row" and "every record
                // arrived once" are the assertions that catch a staging bug.
                uploads.merge(jobId, body(exchange), String::concat);
                respond(exchange, 201, "");

            } else if (method.equals("PATCH") && path.contains("/jobs/ingest/")) {
                respond(exchange, 200, "{\"id\":\"" + segment(path, 1) + "\",\"state\":\"UploadComplete\"}");

            } else if (method.equals("GET") && path.contains("/jobs/ingest/") && path.endsWith("failedResults/")) {
                respondCsv(exchange, failedResults);

            } else if (method.equals("GET") && path.contains("/jobs/ingest/")) {
                String jobId = segment(path, 1);
                int seen = polls.computeIfAbsent(jobId, k -> new AtomicInteger()).getAndIncrement();
                String state = seen < pollsBeforeComplete ? "InProgress" : "JobComplete";
                // The counts Salesforce puts in the status document. A connector that reports
                // failures without downloading the results file reads them from here, so the fake
                // has to carry them or that path cannot be tested at all.
                long failed = failedResults.isBlank()
                        ? 0 : Math.max(0, failedResults.strip().lines().count() - 1);
                respond(exchange, 200, "{\"id\":\"" + jobId + "\",\"state\":\"" + state + "\""
                        + ",\"numberRecordsFailed\":" + failed
                        + ",\"numberRecordsProcessed\":" + (failed + 7) + "}");

            } else if (method.equals("POST") && path.endsWith("/jobs/query")) {
                String id = "query-" + jobCounter.incrementAndGet();
                respond(exchange, 200, "{\"id\":\"" + id + "\",\"state\":\"UploadComplete\"}");

            } else if (method.equals("GET") && path.endsWith("/results")) {
                serveQueryPage(exchange, query);

            } else if (method.equals("GET") && path.contains("/jobs/query/")) {
                respond(exchange, 200, "{\"id\":\"" + segment(path, 1) + "\",\"state\":\"JobComplete\"}");

            } else if (method.equals("DELETE")) {
                deleted.add(segment(path, 1));
                respond(exchange, 204, "");

            } else if (method.equals("GET") && path.contains("/jobs/ingest")) {
                respond(exchange, 200, "{\"records\":[]}");

            } else {
                respond(exchange, 404, "{\"message\":\"no route for " + method + " " + path + "\"}");
            }
        } catch (RuntimeException e) {
            respond(exchange, 500, "{\"message\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * Serves one page and names the next in {@code Sforce-Locator}.
     *
     * <p>The last page answers the literal string {@code "null"}, exactly as Salesforce does —
     * which is the detail the connector has to treat as "no more pages" rather than sending back
     * as a locator.
     */
    private void serveQueryPage(HttpExchange exchange, String query) throws IOException {
        int index = 0;
        if (query != null && query.contains("locator=")) {
            String locator = query.replaceAll(".*locator=([^&]*).*", "$1");
            index = Integer.parseInt(locator);
        }

        String csv = index < queryPages.size() ? queryPages.get(index) : "";
        String next = index + 1 < queryPages.size() ? String.valueOf(index + 1) : "null";

        exchange.getResponseHeaders().add("Sforce-Locator", next);
        respondCsv(exchange, csv);
    }

    private static String segment(String path, int fromEnd) {
        String[] parts = path.split("/");
        return parts[parts.length - fromEnd];
    }

    private static String body(HttpExchange exchange) {
        try (var in = exchange.getRequestBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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

    private static void respondCsv(HttpExchange exchange, String csv) throws IOException {
        byte[] bytes = csv.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/csv");
        exchange.sendResponseHeaders(200, bytes.length == 0 ? -1 : bytes.length);
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
