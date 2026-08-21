package com.dmp.domain.connector;

import com.dmp.common.error.DmpException;
import com.dmp.common.json.Json;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A connector instance may hold references to credentials, never credentials.
 *
 * <p>This aggregate is returned by the API in full, written to the audit log, and included in every
 * backup of the definition store. That is only safe while nothing in it is secret, and the field
 * most likely to break that is the one whose label says "credential" — someone pastes the password
 * they were given, it fails to resolve, and the message says a configuration is missing rather than
 * that a credential is now in three places it should never be.
 */
class SecretReferenceTest {

    private static final TenantId TENANT = TenantId.newId();

    @Test
    void referencesAreAccepted() {
        assertThatCode(() -> instance(refs -> {
            refs.put("username", "env:PG_USER");
            refs.put("password", "env:PG_PASSWORD");
            refs.put("token", "vault:finance/postgres#token");
        })).doesNotThrowAnyException();
    }

    @Test
    void aPastedCredentialIsRefused() {
        // The real one, from a Salesforce consumer secret: uppercase hex, no colon anywhere. It
        // used to be stored verbatim and then looked up as though it were a variable name.
        assertThatThrownBy(() -> instance(refs ->
                refs.put("clientSecret", "DB424F620F6A04BE25CDB9365323718D20EE072CCBCC3978103E3B0C2")))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("clientSecret")
                .hasMessageContaining("env:CLIENT_SECRET");
    }

    @Test
    void theRefusalDoesNotRepeatTheValueItRefused() {
        // The message travels to the API response and the log — the two places this rule exists to
        // keep a credential out of. Echoing it back would defeat the rule while enforcing it.
        String secret = "hunter2-the-actual-password";

        assertThatThrownBy(() -> instance(refs -> refs.put("password", secret)))
                .isInstanceOf(DmpException.class)
                .extracting(Throwable::getMessage)
                .asString()
                .doesNotContain(secret);
    }

    @Test
    void aConnectionStringIsRefusedEvenThoughItHasAScheme() {
        // The case that makes an allow-list necessary rather than "anything with a colon". This one
        // has a scheme, a colon and a password in it, and would otherwise sail through as a
        // reference to a store called 'mongodb'.
        assertThatThrownBy(() -> instance(refs ->
                refs.put("password", "mongodb://appuser:s3cr3t@mongo.prod:27017/orders")))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("password");
    }

    @Test
    void anUnknownSchemeIsRefusedRatherThanStoredForAStoreThatDoesNotExist() {
        assertThatThrownBy(() -> instance(refs -> refs.put("token", "secretmanager:prod/api-key")))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("token");
    }

    @Test
    void aBlankOrAbsentCredentialIsLeftAlone() {
        // Not every declared credential applies to every instance — a REST connector using bearer
        // auth configures no username. Refusing an empty field would demand a fictitious value.
        assertThatCode(() -> instance(refs -> {
            refs.put("username", "");
            refs.putNull("password");
        })).doesNotThrowAnyException();
    }

    @Test
    void theRuleAppliesToEditsAndNotOnlyToCreation() {
        ConnectorInstance existing = instance(refs -> refs.put("password", "env:PG_PASSWORD"));

        assertThatThrownBy(() -> existing.updateConfiguration("renamed", Json.emptyObject(),
                secretRefs(refs -> refs.put("password", "plaintext-password")),
                null, Instant.EPOCH))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("password");
    }

    @Test
    void aRowStoredBeforeThisRuleStillLoads() {
        // Validated on the write paths rather than in the canonical constructor, so an instance
        // saved when this was allowed can still be read and corrected. Making it unreadable would
        // turn a data problem into an outage.
        assertThatCode(() -> new ConnectorInstance(
                ConnectorInstanceId.newId(), TENANT, "legacy", "mongodb", ConnectorDirection.BOTH,
                Json.emptyObject(), secretRefs(refs -> refs.put("password", "a-raw-value")),
                ConnectorInstanceStatus.ACTIVE, null, null, null, Instant.EPOCH, Instant.EPOCH, 0L))
                .doesNotThrowAnyException();
    }

    @Test
    void aCorrectedInstanceSavesCleanly() {
        ConnectorInstance corrected = new ConnectorInstance(
                ConnectorInstanceId.newId(), TENANT, "legacy", "mongodb", ConnectorDirection.BOTH,
                Json.emptyObject(), secretRefs(refs -> refs.put("password", "a-raw-value")),
                ConnectorInstanceStatus.ACTIVE, null, null, null, Instant.EPOCH, Instant.EPOCH, 0L)
                .updateConfiguration("legacy", Json.emptyObject(),
                        secretRefs(refs -> refs.put("password", "env:PG_PASSWORD")),
                        null, Instant.EPOCH);

        assertThat(corrected.secretRefs().get("password").asText()).isEqualTo("env:PG_PASSWORD");
    }

    private static ConnectorInstance instance(java.util.function.Consumer<ObjectNode> customise) {
        return ConnectorInstance.create(TENANT, "test", "mongodb", ConnectorDirection.BOTH,
                Json.emptyObject(), secretRefs(customise), null, Instant.EPOCH);
    }

    private static ObjectNode secretRefs(java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode refs = Json.newObject();
        customise.accept(refs);
        return refs;
    }
}
