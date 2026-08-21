package com.dmp.connector.salesforce;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Bulk API 2.0 job lifecycle, driven end to end against a local stand-in.
 *
 * <p>These assertions are about <em>sequence</em>, because that is what a bulk connector gets wrong.
 * Uploading per batch instead of once multiplies the org's job quota by the number of batches;
 * polling before closing the job waits for something that will never finish; treating the first
 * "InProgress" as an answer reports success for records Salesforce has not looked at yet.
 *
 * <p>What this cannot show is whether Salesforce really behaves this way — see {@link FakeSalesforce}.
 */
class SalesforceLifecycleTest {

    @Test
    void anIngestChunkIsOneJobUploadedOnce() throws Exception {
        try (FakeSalesforce salesforce = new FakeSalesforce()) {
            SalesforceConnector connector = new SalesforceConnector();

            try (Sink.SinkSession sink = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "insert");
            }))) {

                Preparation job = sink.prepare();
                assertThat(job.state().path("jobId").asText()).startsWith("job-");

                // Three batches into one job. A connector that uploaded per batch would create
                // three jobs, which is the mistake this asserts against.
                sink.write(batch(0, "Acme", "Globex"));
                sink.write(batch(2, "Initech"));
                sink.write(batch(3, "Umbrella"));

                assertThat(salesforce.calls())
                        .as("nothing may be uploaded until commit; write only stages")
                        .noneMatch(call -> call.startsWith("PUT"));

                sink.commit(job);

                String uploaded = salesforce.uploadFor(job.state().path("jobId").asText());
                assertThat(uploaded.lines().count())
                        .as("one header row plus four records")
                        .isEqualTo(5);
                assertThat(uploaded.lines().findFirst().orElseThrow())
                        .as("the header is written once, not per batch")
                        .isEqualTo("Name");
                assertThat(uploaded).contains("Acme", "Globex", "Initech", "Umbrella");

                assertThat(salesforce.calls())
                        .filteredOn(call -> call.startsWith("PUT"))
                        .as("one upload for the whole chunk")
                        .hasSize(1);
            }
        }
    }

    @Test
    void pollingContinuesUntilSalesforceSaysTheJobIsDone() throws Exception {
        try (FakeSalesforce salesforce = new FakeSalesforce().pollsBeforeComplete(3)) {
            SalesforceConnector connector = new SalesforceConnector();

            try (Sink.SinkSession sink = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "insert");
            }))) {
                Preparation job = sink.prepare();
                sink.write(batch(0, "Acme"));
                sink.commit(job);

                // Exactly what the engine does: poll, and believe only JobComplete.
                List<Preparation.Status> answers = new ArrayList<>();
                for (int attempt = 0; attempt < 5; attempt++) {
                    Preparation.Status status = sink.checkCommit(job);
                    answers.add(status);
                    if (status.isReady()) {
                        break;
                    }
                }

                assertThat(answers).hasSize(4);
                assertThat(answers.subList(0, 3))
                        .as("an InProgress job must not be reported ready")
                        .allMatch(status -> !status.isReady() && !status.isFailed());
                assertThat(answers.get(3).isReady()).isTrue();

                assertThat(answers.get(0).retryAfter().toSeconds())
                        .as("a poll interval must be offered, or the engine would spin")
                        .isPositive();
            }
        }
    }

    @Test
    void harvestReportsSalesforcesOwnCountsAndCopiesNoRecords() throws Exception {
        // The refused rows stay in the org. Pulling them here would copy customer data into a
        // second store — with its own redaction, retention and erasure story — to answer a
        // question nobody has asked yet, on every run, whether or not anyone ever looks. The job
        // id on the chunk is the handle; Salesforce's own file sits behind it.
        String failures = """
                sf__Id,sf__Error,Name,ExternalId__c
                ,DUPLICATE_VALUE: duplicate value found: ExternalId__c,Acme,A-1
                ,REQUIRED_FIELD_MISSING: Required fields are missing: [Name],,A-2
                ,DUPLICATE_VALUE: duplicate value found: ExternalId__c,Globex,A-3
                """;

        try (FakeSalesforce salesforce = new FakeSalesforce().failedResults(failures)) {
            SalesforceConnector connector = new SalesforceConnector();

            try (Sink.SinkSession sink = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "insert");
            }))) {
                Preparation job = sink.prepare();
                sink.write(batch(0, "Acme"));
                sink.commit(job);

                Sink.Harvest harvest = sink.harvest(job);

                assertThat(harvest.errors())
                        .as("no record leaves the org through this path")
                        .isEmpty();
                assertThat(harvest.failedCount())
                        .as("the count is Salesforce's own, and is what the chunk records")
                        .isEqualTo(salesforce.reportedFailedCount());
            }
        }
    }

    @Test
    void releaseKeepsTheJobSoItsResultsCanStillBeDownloaded() throws Exception {
        // Deleting the job destroys the failed-records file, and the platform keeps no copy of it.
        try (FakeSalesforce salesforce = new FakeSalesforce()) {
            SalesforceConnector connector = new SalesforceConnector();

            try (Sink.SinkSession sink = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "insert");
            }))) {
                Preparation job = sink.prepare();
                sink.write(batch(0, "Acme"));
                sink.commit(job);
                sink.release(job);

                assertThat(salesforce.deletedJobs())
                        .as("the org ages job data out on its own; the platform must not hurry it")
                        .isEmpty();
            }
        }
    }

    /**
     * The default: report how many were refused, download nothing.
     *
     * <p>Salesforce puts the failure count in the status document the engine is already polling.
     * Naming <em>which</em> records failed means fetching a file holding every rejected row — 719 KB
     * for five thousand records, and proportionally more for a large chunk — which is pure cost
     * when the pipeline is not going to keep those payloads anyway.
     *
     * <p>The assertion that matters is the negative one: no request to {@code failedResults}. A
     * connector that downloaded and then discarded would pass every count-based check while doing
     * exactly the work this exists to avoid.
     */
    @Test
    void withoutCollectionTheCountComesFromTheStatusAndNothingIsDownloaded() throws Exception {
        String failures = """
                sf__Id,sf__Error,Name
                ,STORAGE_LIMIT_EXCEEDED: storage limit exceeded,Acme
                ,STORAGE_LIMIT_EXCEEDED: storage limit exceeded,Globex
                """;

        try (FakeSalesforce salesforce = new FakeSalesforce().failedResults(failures)) {
            SalesforceConnector connector = new SalesforceConnector();

            try (Sink.SinkSession sink = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "insert");
                // collectRecordResults deliberately not set — the default is what is under test.
            }))) {
                Preparation job = sink.prepare();
                sink.write(batch(0, "Acme"));
                sink.commit(job);

                Sink.Harvest harvest = sink.harvest(job);

                assertThat(harvest.failedCount())
                        .as("the count is Salesforce's own, taken from the status document")
                        .isEqualTo(2);
                assertThat(harvest.hasDetail())
                        .as("no payloads were collected, so there is nothing to replay")
                        .isFalse();

                assertThat(salesforce.calls())
                        .as("the results file must not be fetched merely to count its rows")
                        .noneMatch(call -> call.contains("failedResults"));
            }
        }
    }

    @Test
    void insertIsHonestAboutNotBeingIdempotent() throws Exception {
        try (FakeSalesforce salesforce = new FakeSalesforce()) {
            SalesforceConnector connector = new SalesforceConnector();

            try (Sink.SinkSession insert = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "insert");
            }))) {
                assertThat(insert.capabilities().writeIsIdempotent()).isFalse();
                assertThat(insert.capabilities().advice()).isPresent();
                assertThat(insert.capabilities().advice().orElseThrow())
                        .as("the advice must say what to do, not merely that there is a problem")
                        .contains("upsert");
            }

            try (Sink.SinkSession upsert = connector.openSink(context(salesforce, node -> {
                node.put("object", "Account");
                node.put("operation", "upsert");
                node.put("externalIdField", "ExternalId__c");
            }))) {
                assertThat(upsert.capabilities().writeIsIdempotent()).isTrue();
                assertThat(upsert.capabilities().commitIsAsynchronous())
                        .as("a successful write still does not mean the records landed")
                        .isTrue();
            }
        }
    }

    @Test
    void queryResultsArePagedByTheLocatorSalesforceReturns() throws Exception {
        List<String> pages = List.of(
                "Id,Name\n001A,Acme\n001B,Globex\n",
                "Id,Name\n001C,Initech\n001D,Umbrella\n",
                "Id,Name\n001E,Soylent\n");

        try (FakeSalesforce salesforce = new FakeSalesforce().queryPages(pages)) {
            SalesforceConnector connector = new SalesforceConnector();

            Source.SourceSession source = connector.openSource(context(salesforce, node ->
                    node.put("soql", "SELECT Id, Name FROM Account")));

            Preparation job = source.prepare();
            assertThat(source.checkPreparation(job).isReady()).isTrue();

            List<Source.SplitSpec> splits = source.plan(job, new Source.PlanRequest(1000, 100));
            assertThat(splits).hasSize(1);

            List<String> names = new ArrayList<>();
            try (Source.RecordStream stream = source.read(splits.get(0), null, 2)) {
                DataRecord record;
                while ((record = stream.next()) != null) {
                    names.add(record.payload().path("Name").asText());
                }
            }

            assertThat(names)
                    .as("every page must be followed, not just the first")
                    .containsExactly("Acme", "Globex", "Initech", "Umbrella", "Soylent");
        }
    }

    @Test
    void aPartlyReadPageResumesFromItsOwnStartRatherThanSkippingTheRest() throws Exception {
        List<String> pages = List.of(
                "Id,Name\n001A,Acme\n001B,Globex\n",
                "Id,Name\n001C,Initech\n");

        try (FakeSalesforce salesforce = new FakeSalesforce().queryPages(pages)) {
            SalesforceConnector connector = new SalesforceConnector();

            Source.SourceSession source = connector.openSource(context(salesforce, node ->
                    node.put("soql", "SELECT Id, Name FROM Account")));

            Preparation job = source.prepare();
            Source.SplitSpec split = source.plan(job, new Source.PlanRequest(1000, 100)).get(0);

            JsonNode cursorAfterOneRecord;
            try (Source.RecordStream stream = source.read(split, null, 2)) {
                assertThat(stream.next().payload().path("Name").asText()).isEqualTo("Acme");
                // A checkpoint here: one record of a two-record page has been handed out.
                cursorAfterOneRecord = stream.cursor();
            }

            List<String> afterResume = new ArrayList<>();
            try (Source.RecordStream stream = source.read(split, cursorAfterOneRecord, 2)) {
                DataRecord record;
                while ((record = stream.next()) != null) {
                    afterResume.add(record.payload().path("Name").asText());
                }
            }

            // Acme arrives twice, which is at-least-once and expected. What must not happen is
            // Globex going missing — reporting the next page's locator mid-page would have skipped
            // it silently, and silent loss is the one outcome this platform refuses.
            assertThat(afterResume)
                    .as("the unread remainder of the page must not be skipped")
                    .contains("Globex", "Initech");
        }
    }

    // ------------------------------------------------------------------ setup

    private static RecordBatch batch(long firstSeq, String... names) {
        List<DataRecord> records = new ArrayList<>();
        long seq = firstSeq;
        for (String name : names) {
            ObjectNode payload = Json.newObject();
            payload.put("Name", name);
            records.add(DataRecord.of(payload, name, ++seq));
        }
        return RecordBatch.of(records);
    }

    private static ConnectorContext context(FakeSalesforce salesforce,
                                            java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode config = Json.newObject();
        config.put("loginUrl", salesforce.url());
        config.put("apiVersion", FakeSalesforce.API);
        config.put("pollSeconds", 1);
        customise.accept(config);

        return new ConnectorContext() {
            @Override
            public JsonNode config() {
                return config;
            }

            @Override
            public Optional<String> secret(String name) {
                return switch (name) {
                    case "clientId" -> Optional.of("fake-client");
                    case "clientSecret" -> Optional.of("fake-secret");
                    default -> Optional.empty();
                };
            }

            @Override
            public String workerId() {
                return "test-worker";
            }

            @Override
            public String runId() {
                return "test-run";
            }

            @Override
            public org.slf4j.Logger log() {
                return LoggerFactory.getLogger(SalesforceLifecycleTest.class);
            }
        };
    }
}
