# ADR-0008: GraalJS transformation sandbox and its enforceable limits

**Status:** Accepted (2026-08-06)

## Context

Users write JavaScript transformations in the UI. That code is untrusted and runs inside the
data plane. It must be able to manipulate JSON and nothing else — no JVM classes, no threads,
no filesystem, no network, no reflection, no `Runtime`, no `ProcessBuilder`.

## Decision

GraalJS via the polyglot API, with a locked-down context:

```java
Context.newBuilder("js")
    .engine(sharedEngine)                 // shares the parsed-AST cache across executions
    .allowAllAccess(false)
    .allowHostAccess(HostAccess.NONE)
    .allowHostClassLookup(c -> false)
    .allowIO(IOAccess.NONE)
    .allowNativeAccess(false)
    .allowCreateThread(false)
    .allowCreateProcess(false)
    .allowEnvironmentAccess(EnvironmentAccess.NONE)
    .allowPolyglotAccess(PolyglotAccess.NONE)
    .option("js.ecmascript-version", "2023")
    .build();
```

One fresh `Context` per execution, off a shared `Engine`. The shared engine is what makes
this fast — the script is parsed once and the AST cache is reused, while each execution gets
a clean global scope.

Payloads cross the boundary as `JsonNode` (ADR-0003), which requires no host access to
marshal.

Alongside JavaScript, declarative nodes (Mapper, Filter, Flatten) backed by JSONata cover the
majority of real transformations, which are field mapping. Those pay none of the sandbox's
cost and are statically analysable for the pipeline validator.

## The limitation, stated plainly

Hard CPU-time and heap limits — `sandbox.MaxCPUTime`, `sandbox.MaxHeapMemory`,
`sandbox.MaxStatements` — are **Oracle GraalVM features, not Community Edition**. On CE the
enforceable controls are:

- A watchdog thread calling `Context.close(true)`, which interrupts the guest execution.
- Statement-count instrumentation via the polyglot instrumentation API.

Together these stop infinite loops and runaway recursion. They do **not** stop a single
statement allocating a multi-gigabyte array. That allocation happens on the host heap and
the watchdog sees a JVM under memory pressure, not a script to interrupt.

## Containment

Because the limit cannot be enforced in-context on CE, it is enforced at the process level.
Transformations execute in a bounded worker pool within the data plane, and the data plane
runs with a configured heap ceiling and an OOM-kill policy. A hostile or merely careless
script degrades or kills one worker process; the control plane, other workers, and every
other run are unaffected.

This is why ADR-0004 keeps the control plane and data plane separable even while they ship
as one artifact — this is the failure mode that boundary exists for.

## Escalation options, if process-level containment proves insufficient

| Option | Gain | Cost |
|---|---|---|
| Polyglot isolates (`--engine.SpawnIsolate=true`) | True memory isolation per context, guest heap bounded independently | Higher per-execution overhead; feature maturity should be validated before committing |
| Oracle GraalVM under GFTC | Real `sandbox.*` resource limits, enforced in-context | Licensing review required; free for production under the GraalVM Free Terms and Conditions but not the same licence as CE |
| External transformation service | Complete blast-radius isolation, independently scalable | Network hop per record batch, another deployable, materially higher latency |

These are operational and licensing decisions rather than engineering ones. The architecture
does not preclude any of them: transformation execution sits behind the `Transform` SPI in
`dmp-transform-api`.

## Consequences

**Positive**
- Escape-proof against the entire class of attacks the requirement named: no host classes,
  no IO, no threads, no reflection, no process execution.
- Shared-engine AST caching makes per-record execution cost acceptable for high throughput.
- The JSONata path means most users never touch the sandbox at all.

**Negative**
- Resource exhaustion is contained rather than prevented on Community Edition. This must be
  documented for operators, not hidden.
- A per-execution `Context` has non-zero setup cost even with a shared engine. Benchmarking
  against a pooled-context alternative is a Phase 5 task; pooling would require proving global
  scope can be reset completely, which is a security claim needing evidence.

## Verification

Phase 5 ships an adversarial test suite as a release gate: attempted host class lookup,
`java.lang.Runtime` access, filesystem reads, socket creation, thread spawning, prototype
pollution, infinite loops, deep recursion, and large allocation. Each must fail in the
expected way and the failure mode must be asserted, not merely observed.
