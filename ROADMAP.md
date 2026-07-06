# OpenEverest Streaming Stack — Roadmap

This repo (`provider-strimzi-kafka`) is the **Apache Kafka broker** layer of a four-part
streaming stack for [OpenEverest](https://github.com/openeverest/openeverest). It wraps the
[Strimzi](https://strimzi.io) operator (KRaft mode, no ZooKeeper) to expose Kafka through the
standard OpenEverest provider / managed-Instance API.

## Source of discussion

The streaming stack is tracked as four feature requests on `openeverest/openeverest`. These are
the source of record for the design discussion:

| Ticket | Layer | Scope |
|--------|-------|-------|
| [#2335](https://github.com/openeverest/openeverest/issues/2335) | Broker (infra) | Redpanda managed broker → [`provider-redpanda`](https://github.com/scaledb-io/provider-redpanda) |
| [#2336](https://github.com/openeverest/openeverest/issues/2336) | Broker (infra) | Apache Kafka / Strimzi managed broker → **this repo** |
| [#2337](https://github.com/openeverest/openeverest/issues/2337) | Compute (infra) | Kafka Connect cluster management |
| [#2338](https://github.com/openeverest/openeverest/issues/2338) | Value (payoff) | Debezium CDC connectors (MySQL, PostgreSQL) |

The infrastructure layers (broker + Connect) exist to unlock the user-facing payoff: going from a
running MySQL/PostgreSQL instance to a live CDC stream without leaving Everest.

## Architecture decisions

### 1. Connect is its own layer, not part of the broker
A broker (Kafka / Redpanda) is stateful data infrastructure that maps onto Everest's managed-Instance
model. Kafka Connect is a **stateless worker runtime** that depends on a broker and runs on top of it.
It is modeled as a **separate provider**, with a clean one-way dependency
(Connect → broker bootstrap servers).

### 2. The Connect provider is broker-agnostic (pluggable backend)
One Connect layer serves both Strimzi-Kafka and Redpanda:
- **Strimzi present:** manage Strimzi `KafkaConnect` / `KafkaConnector` CRs and let the operator do
  the heavy lifting.
- **Redpanda / no Strimzi:** standalone Connect `Deployment` / `StatefulSet` driven via the Connect
  REST API (Redpanda ships no Connect operator).

### 3. Sequencing — ship the value path first
Build the Strimzi `KafkaConnect` path end-to-end (including the Debezium golden path, #2338) first,
then generalize to the Redpanda standalone backend.

### 4. Plugin distribution
- **Baseline (portable):** an init-container pulls plugin JARs (URLs / OCI artifacts) into a shared
  plugin volume — backend-agnostic, works everywhere.
- **Optimization (Strimzi only):** Strimzi's declarative `KafkaConnect.spec.build` image-baking when a
  build registry is available.
- **Paved road:** a curated, pre-bundled Debezium plugin path for the #2338 CDC flow.

### Open question for OpenEverest core
Does Everest's model have a first-class home for a **non-database workload** like a Connect cluster,
or is everything currently shaped as a DB "Instance"? This affects how a Connect cluster surfaces in
the API/UI and should be settled before the Connect CRD / API handlers are finalized.

## Direction posted upstream
The architecture direction above was shared with the community contributor working #2337 here:
<https://github.com/openeverest/openeverest/issues/2337#issuecomment-4890952160>

## Status

| Component | Repo | State |
|-----------|------|-------|
| Redpanda broker | [`provider-redpanda`](https://github.com/scaledb-io/provider-redpanda) | Built + live-validated (Redpanda Operator v26.1.5) |
| Kafka / Strimzi broker | this repo | Scaffolded (Strimzi 1.0.0, KRaft, Kafka 4.2) — not yet live-validated |
| Kafka Connect | — | Design; contributor engaged on #2337 |
| Debezium CDC | — | Not started (#2338) |

---
_Licensing: this Strimzi-based provider and the Strimzi operator are both Apache-2.0.
`provider-redpanda` wraps a BSL-1.1 operator (the provider code itself is Apache-2.0)._
