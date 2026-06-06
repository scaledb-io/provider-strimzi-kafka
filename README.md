# provider-strimzi-kafka

An [OpenEverest](https://github.com/openeverest) provider for Apache Kafka.

Wraps the **[Strimzi Kafka Operator](https://strimzi.io)** and runs Kafka in **KRaft mode** (no ZooKeeper required).

> **License:** Apache Kafka and the Strimzi operator are both Apache 2.0.
> This provider is also Apache 2.0 — see [LICENSE](LICENSE).

> **New to provider development?** See `github.com/openeverest/provider-sdk/blob/main/PROVIDER_DEVELOPMENT.md` for a complete guide.

---

## Prerequisites

- Go 1.26+
- A Kubernetes cluster (k3d, kind, or remote)
- [OpenEverest v2 CRDs](https://github.com/openeverest/openeverest) installed
- [Strimzi Kafka Operator](https://strimzi.io/docs/operators/latest/deploying.html) installed

### Install Strimzi

```bash
helm repo add strimzi https://charts.strimzi.io/index.yaml && helm repo update
helm install strimzi strimzi/strimzi-kafka-operator --namespace strimzi --create-namespace
```

---

## Quick Start

```bash
# Generate all manifests (RBAC, provider spec, Helm chart)
make generate

# Run the provider locally (for development)
make run

# Or deploy with Helm
make helm-install
```

---

## Supported Versions

| Kafka Version | Strimzi Image | Default | Notes |
|--------------|---------------|---------|-------|
| 4.0.0 | `quay.io/strimzi/kafka:0.47.0-kafka-4.0.0` | ✅ | ZooKeeper removed — KRaft only |
| 3.9.1 | `quay.io/strimzi/kafka:0.47.0-kafka-3.9.1` | | |
| 3.8.1 | `quay.io/strimzi/kafka:0.45.0-kafka-3.8.1` | | |

---

## Topologies

### `standalone`
Single-broker Kafka cluster. KRaft mode, no ZooKeeper.
Suitable for development and local testing. **Not recommended for production.**

### `replicated`
Multi-broker Kafka cluster. KRaft mode, minimum 3 brokers (Raft quorum).
Replication factors and min.insync.replicas are set to 3 automatically.
Suitable for production workloads requiring high availability.

---

## Architecture

```
OpenEverest Instance CR
      ↓
provider-strimzi-kafka (this provider)
      ↓  creates
Strimzi Kafka CR (KRaft mode — no ZooKeeper)
      ↓  managed by
Strimzi Operator
      ↓  provisions
Kafka broker Pods + bootstrap Service
```

**KRaft mode** means the Kafka cluster manages its own metadata via the Raft consensus protocol. No external ZooKeeper cluster is required — all nodes participate in both broker and controller roles.

### Connection

Once ready, the Kafka bootstrap endpoint is:
```
<instance>-kafka-bootstrap.<namespace>.svc:9092
```

---

## Project Structure

```
cmd/provider/              # Entry point
internal/
  provider/
    provider.go            # ProviderInterface implementation (Validate/Sync/Status/Cleanup)
    rbac.go                # Kubebuilder RBAC markers for Strimzi CRDs
  common/
    spec.go                # Component name constants
definition/
  provider.yaml            # Provider name + component→type mapping
  versions.yaml            # Supported Kafka versions + Strimzi images
  topologies/
    standalone/            # Single-broker dev topology
    replicated/            # 3-broker production topology (KRaft quorum)
```

---

## Related Issues

- [openeverest/openeverest#2336](https://github.com/openeverest/openeverest/issues/2336) — Add support for Apache Kafka / Strimzi
- [openeverest/openeverest#2337](https://github.com/openeverest/openeverest/issues/2337) — Kafka Connect cluster management
- [openeverest/openeverest#2338](https://github.com/openeverest/openeverest/issues/2338) — Debezium CDC connector support
