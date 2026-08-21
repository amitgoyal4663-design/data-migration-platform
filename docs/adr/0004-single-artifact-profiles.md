# ADR-0004: One deployable artifact, two roles by Spring profile

**Status:** Accepted (2026-08-06)

## Context

The control plane (API, orchestration, scheduling) and the data plane (connectors,
transformations, bulk data movement) have different scaling curves, different failure blast
radii and different JVM tuning needs. A runaway transformation script must never take down
the API the UI depends on.

That argues for two deployables. It also doubles build, image and configuration overhead
during the phases where iteration speed matters most.

## Decision

One Spring Boot artifact, `dmp-app`, with three profiles:

| Profile | Role |
|---|---|
| `control-plane` | REST/OpenAPI, pipeline CRUD, versioning, validation, scheduling, run orchestration |
| `worker` | Plugin classloading, DAG execution, connectors, transformations, checkpointing |
| `all` | Both, for local development only |

Docker Compose and Kubernetes run it as two separate deployments from the same image.

Critically, the two roles communicate **only** through Kafka and the datastores — never
through in-process method calls, even when co-located under the `all` profile. This is
enforced by module dependency structure: no module used exclusively by the control plane
may be depended upon by an engine module, and vice versa. An ArchUnit test enforces it.

## Consequences

**Positive**
- Single build, single image, single configuration surface through the early phases.
- `all` profile means a developer runs the entire platform with one command.
- Because the communication contract is already Kafka-mediated, splitting into two
  artifacts later is a build-file change, never a refactor.

**Negative**
- The `all` profile makes it *possible* to accidentally introduce a direct in-process call
  between roles. Mitigated by the ArchUnit test, which fails the build rather than relying
  on review.
- Both roles carry each other's dependencies in the shared jar, so image size and startup
  classpath are larger than necessary for either role alone. Acceptable; revisited when the
  control plane moves to native image (ADR-0007).
- A single artifact means both roles version together. Fine while the Kafka message schemas
  are still moving; will need reconsideration once external plugin authors depend on stability.

## Trigger for revisiting

Split into two artifacts when any of the following occurs:

- The control plane moves to GraalVM native image (ADR-0007), which the worker cannot follow.
- Worker and control-plane release cadences diverge.
- Worker JVM tuning (heap, GC, off-heap) becomes materially incompatible with the control plane's.
