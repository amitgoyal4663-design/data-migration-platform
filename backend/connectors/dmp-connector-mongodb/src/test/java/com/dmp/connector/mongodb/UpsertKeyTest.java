package com.dmp.connector.mongodb;

import com.dmp.connector.api.ConnectorException;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An upsert needs something to match on.
 *
 * <p>When the key field was missing the filter became {@code {_id: null}}, every record in the
 * migration matched that same one document and replaced it in turn, and MongoDB copied the null
 * into the document it eventually inserted. The run reported COMPLETED with the full record count
 * written and the collection held one row. A whole migration collapsed into a single document and
 * called it a success — which is the worst thing this connector can do.
 */
class UpsertKeyTest {

    @Test
    void aRecordCarryingTheKeyMatchesOnIt() {
        var filter = MongoConnector.matchOnKey(
                new Document("_id", "rec-1").append("name", "Acme"), "_id");

        assertThat(filter.toBsonDocument().toJson()).contains("rec-1");
    }

    @Test
    void aRecordWithNoValueForTheKeyIsRefused() {
        assertThatThrownBy(() -> MongoConnector.matchOnKey(
                new Document("name", "Acme").append("key", "rec-1"), "_id"))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("'_id'");
    }

    @Test
    void theRefusalListsTheFieldsThatAreThere() {
        // So the fix is visible without going to look: the record plainly carries 'key', and the
        // sink is plainly matching on something else.
        assertThatThrownBy(() -> MongoConnector.matchOnKey(
                new Document("name", "Acme").append("key", "rec-1"), "_id"))
                .hasMessageContaining("key")
                .hasMessageContaining("name");
    }

    @Test
    void anExplicitNullIsRefusedTheSameAsAMissingField() {
        // A transform returning { _id: null } produces the identical collapse, and looks even more
        // like it was meant.
        assertThatThrownBy(() -> MongoConnector.matchOnKey(
                new Document("_id", null).append("name", "Acme"), "_id"))
                .isInstanceOf(ConnectorException.class);
    }
}
