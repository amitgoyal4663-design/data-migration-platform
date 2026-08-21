package com.dmp.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architectural boundaries, enforced as build failures rather than review conventions.
 *
 * <p>Every rule here corresponds to a decision recorded in an ADR. A rule without a reason is a
 * rule someone will eventually delete to make a build pass, so each one names what it protects.
 */
class ArchitectureTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.dmp");
    }

    // -------------------------------------------------------------------------
    // Hexagonal boundaries
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("the domain has no framework on its classpath")
    void domainIsFrameworkFree() {
        // The domain must be the thing being modelled, not a shape the ORM or the web framework
        // can map. Jackson is the one permitted exception: ADR-0003 makes JsonNode the payload
        // model itself, which is a modelling decision rather than a serialisation concern.
        ArchRule rule = noClasses()
                .that().resideInAPackage("com.dmp.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework..",
                        "jakarta.persistence..",
                        "org.hibernate..",
                        "org.apache.kafka..",
                        "com.mongodb..",
                        "jakarta.servlet..")
                .because("the domain must stay independent of every framework (ADR-0004); "
                        + "Jackson is the sole exception, per ADR-0003");

        rule.check(classes);
    }

    @Test
    @DisplayName("the application layer depends on no persistence adapter")
    void applicationDependsOnNoAdapter() {
        // This is what keeps the PostgreSQL/MongoDB split of ADR-0005 invisible above the adapter
        // layer, and what makes either store replaceable without touching a use case.
        ArchRule rule = noClasses()
                .that().resideInAPackage("com.dmp.application..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.dmp.persistence..",
                        "com.dmp.app..")
                .because("the application layer declares ports; adapters implement them (ADR-0005)");

        rule.check(classes);
    }

    @Test
    @DisplayName("no module depends on both persistence adapters")
    void noModuleSpansBothStores() {
        // Definitions live in PostgreSQL and execution data in MongoDB. A class reaching into both
        // is either doing a cross-store join that belongs in the application layer, or writing the
        // same fact to two stores — the failure mode ADR-0005 exists to prevent.
        ArchRule postgresRule = noClasses()
                .that().resideInAPackage("com.dmp.persistence.postgres..")
                .should().dependOnClassesThat().resideInAPackage("com.dmp.persistence.mongo..")
                .because("cross-store coupling belongs in the application layer, not an adapter (ADR-0005)");

        ArchRule mongoRule = noClasses()
                .that().resideInAPackage("com.dmp.persistence.mongo..")
                .should().dependOnClassesThat().resideInAPackage("com.dmp.persistence.postgres..")
                .because("cross-store coupling belongs in the application layer, not an adapter (ADR-0005)");

        postgresRule.check(classes);
        mongoRule.check(classes);
    }

    @Test
    @DisplayName("the domain depends on nothing above it")
    void domainDependsOnNothingAbove() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("com.dmp.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.dmp.application..",
                        "com.dmp.persistence..",
                        "com.dmp.app..")
                .because("dependencies point inward");

        rule.check(classes);
    }

    // -------------------------------------------------------------------------
    // Operational constraints
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("nothing creates a Kafka topic")
    void noTopicCreation() {
        // ADR-0013: service accounts have no authority to create topics, and topics are
        // pre-provisioned by the platform team. The rule covers AdminClient entirely rather than
        // just createTopics, because a convenience helper added "only for tests" is how a creation
        // call reaches production and fails at 03:00.
        ArchRule rule = noClasses()
                .that().resideInAPackage("com.dmp..")
                .should().dependOnClassesThat().haveNameMatching(
                        "org\\.apache\\.kafka\\.clients\\.admin\\.(Admin|AdminClient|KafkaAdminClient|NewTopic|CreateTopicsResult)")
                .because("the platform never creates topics; they are pre-provisioned (ADR-0013)");

        rule.check(classes);
    }

    @Test
    @DisplayName("the audit adapter exposes no way to modify an entry")
    void auditLogIsAppendOnly() {
        // ADR-0011: an audit trail that can be rewritten is not an audit trail. Enforced here, by
        // @Immutable on the entity, by the port's interface, and by a database trigger.
        ArchRule rule = noClasses()
                .that().haveSimpleName("AuditLogAdapter")
                .should().callMethodWhere(
                        com.tngtech.archunit.core.domain.JavaCall.Predicates.target(
                                com.tngtech.archunit.core.domain.properties.HasName.Predicates
                                        .nameMatching("delete.*|remove.*|update.*")))
                .because("the control-plane audit trail is append-only (ADR-0011)");

        rule.check(classes);
    }

    // -------------------------------------------------------------------------
    // Conventions
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("out-ports are interfaces")
    void portsAreInterfaces() {
        ArchRule rule = classes()
                .that().resideInAPackage("com.dmp.application.port.out..")
                .and().haveSimpleNameEndingWith("Repository")
                .should().beInterfaces()
                .because("a port is a contract, not an implementation");

        rule.check(classes);
    }

    @Test
    @DisplayName("controllers do not reach past the application layer")
    void controllersUseServices() {
        // A controller calling a repository skips the transaction boundary and the audit write,
        // producing a change that happened but was never recorded.
        ArchRule rule = noClasses()
                .that().resideInAPackage("com.dmp.app.web..")
                .should().dependOnClassesThat().resideInAPackage("com.dmp.persistence..")
                .because("controllers call application services, which own transactions and auditing");

        rule.check(classes);
    }

    @Test
    @DisplayName("nothing reads the wall clock directly")
    void noDirectClockAccess() {
        // Time arrives as a parameter from an injected Clock, which is what makes assertions about
        // retry backoff, TTL expiry and run duration deterministic instead of flaky.
        ArchRule rule = noClasses()
                .that().resideInAnyPackage("com.dmp.domain..", "com.dmp.application..")
                .should().callMethod(java.time.Instant.class, "now")
                .because("time is injected via Clock so behaviour is testable (PlatformConfig)");

        rule.check(classes);
    }
}
