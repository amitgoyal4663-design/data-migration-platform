package com.dmp.domain.audit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What counts as "the same failure" when twenty thousand records fail together.
 *
 * <p>Get this wrong in one direction and the dead-letter queue is a wall of identical sentences
 * that nobody reads and that costs a copy of the source data to store. Get it wrong in the other
 * and two genuinely different faults are reported as one.
 */
class ErrorSignatureTest {

    @Test
    @DisplayName("the same rule broken by different records is one signature")
    void identifiersDoNotSplitTheGroup() {
        // The whole point. These are twenty thousand records failing one rule, and every message
        // differs only in the id it names.
        String first = ErrorSignature.of("E11000",
                "E11000 duplicate key error collection: dmp.orders index: _id_ dup key: "
                        + "{ _id: \"ORD-88213\" }");
        String second = ErrorSignature.of("E11000",
                "E11000 duplicate key error collection: dmp.orders index: _id_ dup key: "
                        + "{ _id: \"ORD-90114\" }");

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("different rules stay different, even under the same code")
    void distinctFaultsAreNotMerged() {
        String missingField = ErrorSignature.of("FIELD_INTEGRITY_EXCEPTION",
                "Required fields are missing: [LastName]");
        String badReference = ErrorSignature.of("FIELD_INTEGRITY_EXCEPTION",
                "invalid cross reference id");

        assertThat(missingField).isNotEqualTo(badReference);
    }

    @Test
    @DisplayName("the error code alone separates two faults that read alike")
    void codeParticipatesInTheSignature() {
        assertThat(ErrorSignature.of("E11000", "write failed"))
                .isNotEqualTo(ErrorSignature.of("E11001", "write failed"));
    }

    @Test
    @DisplayName("UUIDs, object ids and Salesforce ids are all treated as record-specific")
    void identifierShapesAreNormalised() {
        String withUuid = ErrorSignature.normalise(
                "record 019fdb16-2112-70fe-aeac-e3a8903f195a was rejected");
        String withObjectId = ErrorSignature.normalise(
                "record 507f1f77bcf86cd799439011 was rejected");
        String withSalesforceId = ErrorSignature.normalise(
                "record 003xx000004TmiQAAS was rejected");

        assertThat(withUuid).isEqualTo(withObjectId).isEqualTo(withSalesforceId);
    }

    @Test
    @DisplayName("a missing code still groups, rather than every record becoming its own fault")
    void absentCodeIsHandled() {
        assertThat(ErrorSignature.of(null, "something failed"))
                .isEqualTo(ErrorSignature.of("  ", "something failed"))
                .startsWith("NO_CODE");
    }

    @Test
    @DisplayName("the normalised message stays readable rather than becoming a hash")
    void signatureIsLegible() {
        // It is displayed as the group label. A key nobody can read is a key nobody can act on.
        String normalised = ErrorSignature.normalise(
                "duplicate value found: ExternalId__c duplicates value on record with id: 003xx01");

        assertThat(normalised).contains("duplicate value found").contains("ExternalId__c");
    }

    @Test
    @DisplayName("a runaway message is cut to a bounded length")
    void longMessagesAreBounded() {
        assertThat(ErrorSignature.normalise("x".repeat(5_000))).hasSizeLessThanOrEqualTo(300);
    }

    @Test
    @DisplayName("no message at all is still a valid signature")
    void emptyMessageIsSafe() {
        assertThat(ErrorSignature.of("E11000", null)).isEqualTo("E11000|");
    }
}
