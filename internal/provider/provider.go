// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package provider implements the OpenEverest provider for Apache Kafka via the
// Strimzi operator (https://strimzi.io).
//
// Implementation note:
// Strimzi 1.0.0 serves the Kafka and KafkaNodePool APIs at the stable v1
// version (v1beta2 was removed) and mandates the KRaft + NodePool model:
//   - The Kafka CR carries cluster-wide config (version, metadataVersion,
//     listeners, broker config) but NO longer holds replicas or storage.
//   - Broker/controller replicas, roles, storage and resources live on a
//     KafkaNodePool CR bound to the Kafka cluster via the
//     "strimzi.io/cluster" label.
//
// We build both CRs as unstructured Kubernetes objects (plain
// map[string]interface{}) so the provider has zero dependency on a typed Go
// client pinned to a specific Strimzi API version. This mirrors the sibling
// provider-redpanda implementation.
package provider

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/scaledb-io/provider-strimzi-kafka/internal/common"
)

// kafkaGVK is the GroupVersionKind for the Strimzi Kafka cluster CR.
var kafkaGVK = schema.GroupVersionKind{
	Group:   common.StrimziGroup,
	Version: common.StrimziVersion,
	Kind:    common.KafkaKind,
}

// nodePoolGVK is the GroupVersionKind for the Strimzi KafkaNodePool CR.
var nodePoolGVK = schema.GroupVersionKind{
	Group:   common.StrimziGroup,
	Version: common.StrimziVersion,
	Kind:    common.KafkaNodePoolKind,
}

// Compile-time check.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for Apache Kafka via the Strimzi operator.
type Provider struct {
	controller.BaseProvider
}

// New creates a new Provider instance.
func New() *Provider {
	return &Provider{
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			// No SchemeFuncs needed — we use unstructured objects to avoid
			// depending on a typed Strimzi client for a specific API version.
			SchemeFuncs: nil,
			// NOTE: We intentionally do NOT watch Kafka/KafkaNodePool CRs here.
			// Watching them causes a tight feedback loop: operator updates
			// (finalizers, status) re-trigger Apply, which updates the object,
			// which triggers the Strimzi operator again.
			// Instead, Status() polls via c.Get() on each Instance reconcile,
			// and Sync() returns WaitError while provisioning is in progress.
			WatchConfigs: []controller.WatchConfig{},
		},
	}
}

// Validate checks the Instance spec before reconciliation.
func (p *Provider) Validate(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Validating Kafka instance", "name", c.Name())

	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok {
		return fmt.Errorf("engine component is required")
	}

	if engine.Resources != nil && engine.Resources.Limits != nil {
		lim := engine.Resources.Limits
		if cpu := lim.Cpu(); cpu != nil && !cpu.IsZero() {
			if cpu.Cmp(resource.MustParse("1")) < 0 {
				return fmt.Errorf("engine CPU limit must be at least 1 core")
			}
		}
		if mem := lim.Memory(); mem != nil && !mem.IsZero() {
			if mem.Cmp(resource.MustParse("1Gi")) < 0 {
				return fmt.Errorf("engine memory limit must be at least 1Gi")
			}
		}
	}

	if c.Instance().GetTopologyType() == common.TopologyReplicated {
		if engine.Replicas != nil && *engine.Replicas < 3 {
			return fmt.Errorf("replicated topology requires at least 3 brokers for Raft quorum")
		}
	}

	return nil
}

// Sync creates or waits on the Kafka + KafkaNodePool CRs for the selected topology.
//
// Create-only semantics: once created, Strimzi owns the CRs and we must not
// overwrite its changes on every reconcile. WaitError is returned while
// provisioning is in progress so the runtime requeues after 15s.
//
// Ordering: the KafkaNodePool is applied first, then the Kafka CR. Strimzi
// resolves the pool→cluster binding by label, so either order is accepted, but
// creating the pool first avoids a transient "no node pools" warning on the
// Kafka resource.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	topology := c.Instance().GetTopologyType()
	l.Info("Syncing Kafka instance", "name", c.Name(), "topology", topology)

	existing := newKafkaObj(c.Name(), c.Namespace())
	if err := c.Get(existing, c.Name()); err != nil {
		replicas := brokerReplicas(c)

		pool, buildErr := buildNodePool(c, replicas)
		if buildErr != nil {
			return fmt.Errorf("build KafkaNodePool CR: %w", buildErr)
		}
		if applyErr := c.Apply(pool); applyErr != nil {
			return fmt.Errorf("create KafkaNodePool CR: %w", applyErr)
		}

		kafka, buildErr := buildKafka(c)
		if buildErr != nil {
			return fmt.Errorf("build Kafka CR: %w", buildErr)
		}
		if applyErr := c.Apply(kafka); applyErr != nil {
			return fmt.Errorf("create Kafka CR: %w", applyErr)
		}

		l.Info("Kafka CR and KafkaNodePool created", "name", c.Name(), "brokers", replicas)
		return controller.WaitForDuration("waiting for Strimzi operator to provision Kafka cluster", 15*time.Second)
	}

	return waitForKafka(c, existing)
}

// waitForKafka checks the Kafka CR status and returns a WaitError if not yet ready.
func waitForKafka(c *controller.Context, kafka *unstructured.Unstructured) error {
	l := log.FromContext(c.Context())

	ready, msg := kafkaReadyCondition(kafka)
	if ready {
		l.Info("Kafka cluster is Ready", "name", kafka.GetName())
		return nil
	}

	l.Info("Kafka cluster still provisioning", "name", kafka.GetName(), "message", msg)
	return controller.WaitForDuration(
		fmt.Sprintf("waiting for Strimzi operator to complete Kafka provisioning: %s", msg),
		15*time.Second,
	)
}

// Status reports the current status of the Kafka instance.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	kafka := newKafkaObj(c.Name(), c.Namespace())
	if err := c.Get(kafka, c.Name()); err != nil {
		return controller.Provisioning("Waiting for Kafka CR"), nil
	}

	ready, msg := kafkaReadyCondition(kafka)
	if ready {
		return controller.ReadyWithConnectionDetails(buildConnectionDetails(c)), nil
	}

	if isKafkaFailed(kafka) {
		errMsg := msg
		if errMsg == "" {
			errMsg = "Kafka cluster failed"
		}
		return controller.Failed(errMsg), nil
	}

	return controller.Provisioning(fmt.Sprintf("Cluster is being created: %s", msg)), nil
}

// Cleanup removes the Kafka and KafkaNodePool CRs when the Instance is deleted.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up Kafka instance", "name", c.Name())

	kafka := newKafkaObj(c.Name(), c.Namespace())
	if err := c.Delete(kafka); err != nil {
		return fmt.Errorf("delete Kafka CR: %w", err)
	}

	pool := newNodePoolObj(c.Name(), c.Namespace())
	if err := c.Delete(pool); err != nil {
		return fmt.Errorf("delete KafkaNodePool CR: %w", err)
	}

	l.Info("Kafka instance cleaned up", "name", c.Name())
	return nil
}

// =============================================================================
// Builders
// =============================================================================

// buildKafka constructs an unstructured Strimzi Kafka CR (KRaft mode).
//
// In Strimzi 1.0.0 the Kafka CR holds cluster-wide configuration only; broker
// replicas and storage live on the KafkaNodePool (see buildNodePool). The
// KRaft and node-pool feature annotations are required so the operator treats
// this as a KRaft cluster backed by node pools.
//
// Resulting CR (kafka.strimzi.io/v1):
//
//	metadata:
//	  annotations:
//	    strimzi.io/kraft: enabled
//	    strimzi.io/node-pools: enabled
//	spec:
//	  kafka:
//	    version: <x.y.z>
//	    metadataVersion: <x.y-IVn>
//	    listeners: [plain:9092, tls:9093]
//	    config: {replication factors / min ISR}
//	  entityOperator:
//	    topicOperator: {}
//	    userOperator: {}
func buildKafka(c *controller.Context) (*unstructured.Unstructured, error) {
	engine := c.Instance().Spec.Components[common.ComponentEngine]
	image, err := resolveImage(c, engine)
	if err != nil {
		return nil, err
	}
	kafkaVersion := extractKafkaVersion(image)
	metadataVersion := resolveMetadataVersion(image)
	replicas := brokerReplicas(c)

	listeners := []interface{}{
		map[string]interface{}{"name": "plain", "port": int64(9092), "type": "internal", "tls": false},
		map[string]interface{}{"name": "tls", "port": int64(9093), "type": "internal", "tls": true},
	}

	kafka := newKafkaObj(c.Name(), c.Namespace())
	kafka.SetAnnotations(map[string]string{
		// Enable KRaft mode (no ZooKeeper) and the node-pool model.
		"strimzi.io/kraft":      "enabled",
		"strimzi.io/node-pools": "enabled",
	})
	kafka.Object["spec"] = map[string]interface{}{
		"kafka": map[string]interface{}{
			"version":         kafkaVersion,
			"metadataVersion": metadataVersion,
			"listeners":       listeners,
			"config":          buildKafkaConfig(replicas),
		},
		"entityOperator": map[string]interface{}{
			"topicOperator": map[string]interface{}{},
			"userOperator":  map[string]interface{}{},
		},
	}

	return kafka, nil
}

// buildNodePool constructs an unstructured Strimzi KafkaNodePool CR.
//
// A single dual-role (controller+broker) pool backs the cluster in KRaft mode.
// The pool is bound to its Kafka cluster by the "strimzi.io/cluster" label,
// whose value must equal the Kafka CR name.
//
// Resulting CR (kafka.strimzi.io/v1):
//
//	metadata:
//	  labels:
//	    strimzi.io/cluster: <instance>
//	spec:
//	  replicas: <n>
//	  roles: [controller, broker]
//	  storage:
//	    type: jbod
//	    volumes: [{ id: 0, type: persistent-claim, size: <qty>, ... }]
//	  resources:
//	    requests/limits: { cpu, memory }
func buildNodePool(c *controller.Context, replicas int) (*unstructured.Unstructured, error) {
	engine := c.Instance().Spec.Components[common.ComponentEngine]
	cpu, memory := resolveResources(engine)
	storageSize, storageClass := resolveStorage(engine)

	volume := map[string]interface{}{
		"id":          int64(0),
		"type":        "persistent-claim",
		"size":        storageSize.String(),
		"deleteClaim": false,
	}
	if storageClass != nil && *storageClass != "" {
		volume["class"] = *storageClass
	}

	resources := map[string]interface{}{
		"requests": map[string]interface{}{"cpu": cpu.String(), "memory": memory.String()},
		"limits":   map[string]interface{}{"cpu": cpu.String(), "memory": memory.String()},
	}

	pool := newNodePoolObj(c.Name(), c.Namespace())
	pool.SetLabels(map[string]string{
		common.ClusterLabel: c.Name(),
	})
	pool.Object["spec"] = map[string]interface{}{
		"replicas": int64(replicas),
		"roles":    []interface{}{"controller", "broker"},
		"storage": map[string]interface{}{
			"type":    "jbod",
			"volumes": []interface{}{volume},
		},
		"resources": resources,
	}

	return pool, nil
}

// buildKafkaConfig returns Kafka broker config scaled to the replica count.
// Replication factors are capped at 3 even if more brokers are requested.
func buildKafkaConfig(replicas int) map[string]interface{} {
	rf := replicas
	if rf > 3 {
		rf = 3
	}
	minISR := rf - 1
	if minISR < 1 {
		minISR = 1
	}

	return map[string]interface{}{
		"offsets.topic.replication.factor":         int64(rf),
		"transaction.state.log.replication.factor": int64(rf),
		"transaction.state.log.min.isr":            int64(minISR),
		"default.replication.factor":               int64(rf),
		"min.insync.replicas":                      int64(minISR),
	}
}

// newKafkaObj creates an empty unstructured Kafka CR with the correct GVK.
func newKafkaObj(name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(kafkaGVK)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u
}

// newNodePoolObj creates an empty unstructured KafkaNodePool CR with the correct GVK.
func newNodePoolObj(name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(nodePoolGVK)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u
}

// =============================================================================
// Status helpers
// =============================================================================

// kafkaReadyCondition inspects the Kafka status conditions for the Ready condition.
// Returns (true, "") when ready, or (false, message) when not.
func kafkaReadyCondition(kafka *unstructured.Unstructured) (bool, string) {
	conditions, found, err := unstructured.NestedSlice(kafka.Object, "status", "conditions")
	if err != nil || !found {
		return false, "waiting for status"
	}
	for _, raw := range conditions {
		cond, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(cond, "type")
		if condType != "Ready" {
			continue
		}
		status, _, _ := unstructured.NestedString(cond, "status")
		if status == "True" {
			return true, ""
		}
		msg, _, _ := unstructured.NestedString(cond, "message")
		if msg == "" {
			return false, "waiting for Ready condition"
		}
		return false, msg
	}
	return false, "Ready condition not yet reported"
}

// isKafkaFailed returns true if the Ready condition is False with an Error reason.
func isKafkaFailed(kafka *unstructured.Unstructured) bool {
	conditions, found, err := unstructured.NestedSlice(kafka.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, raw := range conditions {
		cond, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(cond, "type")
		status, _, _ := unstructured.NestedString(cond, "status")
		if condType == "Ready" && status == "False" {
			reason, _, _ := unstructured.NestedString(cond, "reason")
			if strings.Contains(reason, "Error") {
				return true
			}
		}
	}
	return false
}

// =============================================================================
// Connection details
// =============================================================================

// buildConnectionDetails returns the Kafka bootstrap endpoint.
// Strimzi naming convention: <instance>-kafka-bootstrap.<namespace>.svc
func buildConnectionDetails(c *controller.Context) controller.ConnectionDetails {
	host := fmt.Sprintf("%s-kafka-bootstrap.%s.svc", c.Name(), c.Namespace())
	return controller.ConnectionDetails{
		Type:     "kafka",
		Provider: common.ProviderName,
		Host:     host,
		Port:     common.BootstrapPort,
		URI:      fmt.Sprintf("%s:%s", host, common.BootstrapPort),
	}
}

// =============================================================================
// Helpers
// =============================================================================

// brokerReplicas returns the configured replica count or the topology default.
func brokerReplicas(c *controller.Context) int {
	engine := c.Instance().Spec.Components[common.ComponentEngine]
	if engine.Replicas != nil && *engine.Replicas > 0 {
		return int(*engine.Replicas)
	}
	if c.Instance().GetTopologyType() == common.TopologyReplicated {
		return common.DefaultReplicatedReplicas
	}
	return common.DefaultStandaloneReplicas
}

// resolveImage returns the container image for the engine component.
func resolveImage(c *controller.Context, engine corev1alpha1.ComponentSpec) (string, error) {
	if engine.Image != "" {
		return engine.Image, nil
	}
	spec, err := c.ProviderSpec()
	if err != nil {
		return "", fmt.Errorf("get provider spec: %w", err)
	}
	if engine.Version != "" {
		if img := controller.GetImageForVersion(spec, common.ComponentEngine, engine.Version); img != "" {
			return img, nil
		}
	}
	if img := controller.GetDefaultImageForComponent(spec, common.ComponentEngine); img != "" {
		return img, nil
	}
	return "", fmt.Errorf("no image found for engine component")
}

// extractKafkaVersion parses the Kafka version from a Strimzi image tag.
// e.g. "quay.io/strimzi/kafka:1.0.0-kafka-4.2.0" → "4.2.0"
func extractKafkaVersion(image string) string {
	parts := strings.Split(image, "-kafka-")
	if len(parts) == 2 {
		return parts[1]
	}
	// Fallback: use the image tag after the last colon.
	if idx := strings.LastIndex(image, ":"); idx >= 0 {
		return image[idx+1:]
	}
	return "4.2.0"
}

// resolveMetadataVersion derives the KRaft metadata version from the image tag.
func resolveMetadataVersion(image string) string {
	switch {
	case strings.Contains(image, "4.2"):
		return common.KafkaMetadataVersion4_2
	case strings.Contains(image, "4.1"):
		return common.KafkaMetadataVersion4_1
	default:
		return common.DefaultMetadataVersion
	}
}

// resolveResources returns CPU and memory quantities with defaults applied.
func resolveResources(engine corev1alpha1.ComponentSpec) (cpu, memory resource.Quantity) {
	cpu = resource.MustParse("1")
	memory = resource.MustParse("2Gi")
	if engine.Resources == nil || engine.Resources.Limits == nil {
		return
	}
	if v := engine.Resources.Limits.Cpu(); v != nil && !v.IsZero() {
		cpu = v.DeepCopy()
	}
	if v := engine.Resources.Limits.Memory(); v != nil && !v.IsZero() {
		memory = v.DeepCopy()
	}
	return
}

// resolveStorage returns the storage size and optional storage class.
func resolveStorage(engine corev1alpha1.ComponentSpec) (size resource.Quantity, storageClass *string) {
	size = resource.MustParse("10Gi")
	if engine.Storage == nil {
		return
	}
	if !engine.Storage.Size.IsZero() {
		size = engine.Storage.Size.DeepCopy()
	}
	storageClass = engine.Storage.StorageClass
	return
}
