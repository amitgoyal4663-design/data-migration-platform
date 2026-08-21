# ADR-0006: Connector SPI with child-first classloader isolation

**Status:** Accepted (2026-08-06)

## Context

The requirement is that adding a connector must not require changes to existing code. Taken
seriously, that constrains three things: the runtime loading mechanism, the dependency
model, and the user interface.

The last one is where most plugin systems quietly fail. A platform can load a connector jar
dynamically and still require a frontend change to render its configuration form — at which
point it is not really extensible.

## Decision

### The SPI

```java
public interface Connector { ConnectorSpec spec(); }

public interface Source extends Connector {
    SourceSession open(SourceContext ctx);
}
public interface SourceSession extends AutoCloseable {
    SchemaCatalog discover();
    List<Split>   plan(SplitPlanRequest req);
    RecordStream  read(Split split, Checkpoint from);
}

public interface Sink extends Connector {
    SinkSession open(SinkContext ctx);
}
public interface SinkSession extends AutoCloseable {
    SinkCapabilities capabilities();
    WriteResult      write(RecordBatch batch);
    void             commit(CommitContext ctx);
}
```

`read` is pull-based so that back-pressure propagates naturally: a slow sink stops pulling
and the source stops reading, with no buffering policy required in between.

### Spec-driven UI

`ConnectorSpec` returns a JSON Schema describing the connector's configuration. The React
console renders configuration forms from that schema at runtime. A new plugin jar produces
a complete, validated configuration UI with zero frontend changes.

### Classloader isolation

Each connector is a fat jar under `plugins/<name>-<version>/`, loaded by a child-first
`PluginClassLoader` and discovered via `ServiceLoader`. Only `dmp-connector-api`,
`dmp-common` and the JDK are visible from the parent.

Child-first, not parent-first, because the alternative is a single global dependency
resolution across every connector the platform ships. The Oracle JDBC driver and the
MongoDB driver will eventually disagree about a transitive Netty or Guava version, and
parent-first turns that into a platform-wide version pin.

### Compliance kit

`dmp-testkit` provides a Connector TCK. Every connector, first-party or third-party, must pass:

| Contract | Assertion |
|---|---|
| Schema discovery | `discover()` is idempotent and side-effect free |
| Split determinism | `plan()` with identical input yields identical splits |
| Checkpoint resumability | `read(split, checkpoint)` resumes exactly, no gaps or duplicates beyond declared semantics |
| Redelivery | Re-reading from a checkpoint after simulated failure honours declared delivery guarantee |
| Error taxonomy | Failures map to `RETRYABLE` / `FATAL` / `RECORD_LEVEL` correctly |
| Capability honesty | Declared `SinkCapabilities` match observed behaviour |

## Consequences

**Positive**
- A third party can ship a connector as a jar with no coordination with the core team, and
  it works end-to-end including its UI.
- Dependency conflicts are contained to a single connector rather than being a platform concern.
- The TCK makes connector quality mechanical rather than a review judgement, which is what
  makes an ecosystem viable.

**Negative**
- Child-first classloading is genuinely tricky. Classes crossing the boundary must be loaded
  by the parent, which constrains `dmp-connector-api` to types with no heavy transitive deps.
- `dmp-connector-api` becomes a semver-stable public contract. Breaking it breaks every
  third-party connector, so it must be kept deliberately small.
- Memory overhead per loaded plugin, and classloader leaks are a real risk on hot reload.
  Hot reload is therefore explicitly out of scope for v1 — a worker restart is required to
  pick up a plugin change.
- Incompatible with GraalVM native image in the worker process. See ADR-0007.
