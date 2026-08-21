# ADR-0007: GraalVM native image limited to the control plane

**Status:** Accepted (2026-08-06)

## Context

GraalVM native image was specified in the platform requirements. It is also mutually
exclusive with the dynamic connector plugin loading required by ADR-0006: a native image
performs closed-world analysis at build time and cannot classload arbitrary jars at runtime.

This conflict is structural, not a configuration problem. Recording it in Phase 0 rather
than discovering it in Phase 10.

## Decision

| Role | Compilation | Reason |
|---|---|---|
| Control plane | GraalVM native image | Sub-second boot and ~50 MB RSS materially improve Kubernetes scale-out and cost. The control plane loads no plugins, so closed-world analysis is not a constraint. |
| Worker | Standard JVM | Requires dynamic plugin classloading. Also, JIT outperforms AOT for sustained data crunching — the workload where peak throughput matters and startup time does not. |

This makes ADR-0004's "one artifact, two profiles" temporary by design: native image is one
of the named triggers for splitting into two build targets.

An optional third mode is available later for edge or embedded deployments: a *native worker
bundle* built with a fixed set of connectors compiled in at image time. It trades
extensibility for footprint and is not part of the core roadmap.

## Consequences

**Positive**
- The control plane gets the operational benefits that motivated the GraalVM requirement.
- The worker keeps full extensibility and peak throughput.
- Reflection configuration burden is confined to the control plane, where the surface is
  small and stable.

**Negative**
- Two build pipelines and two runtime images once the split happens.
- Native image build times are long (minutes, not seconds), so control-plane CI slows down.
  Mitigation: build native only on release branches; JVM builds for pull requests.
- Any library used by the control plane must be native-image compatible. Spring Boot 3.x
  and Spring AOT handle this for the mainstream stack, but it constrains library choice —
  a factor when selecting the persistence layer.
- The requirement as originally stated ("GraalVM") is only partially satisfied. This is a
  deliberate, documented reduction in scope rather than an omission.

## Note on GraalVM's other use

GraalVM appears twice in this architecture and the two uses are independent. GraalJS for the
transformation sandbox (ADR-0008) runs on a standard JVM via the polyglot API and requires
no native image. Nothing in this ADR affects it.
