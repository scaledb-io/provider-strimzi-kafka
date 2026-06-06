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

package provider

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafkav1beta2 "github.com/RedHatInsights/strimzi-client-go/apis/kafka.strimzi.io/v1beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/scaledb-io/provider-strimzi-kafka/internal/common"
)

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
			SchemeFuncs: []func(*runtime.Scheme) error{
				kafkav1beta2.AddToScheme,
			},
			// NOTE: We intentionally do NOT watch Kafka CRs here.
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

// Sync creates or waits on the Kafka CR for the selected topology.
//
// Create-only semantics: once created, Strimzi owns the Kafka CR and we must
// not overwrite its changes on every reconcile. WaitError is returned while
// provisioning is in progress so the runtime requeues after 15s.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	topology := c.Instance().GetTopologyType()
	l.Info("Syncing Kafka instance", "name", c.Name(), "topology", topology)

	existing := &kafkav1beta2.Kafka{}
	if err := c.Get(existing, c.Name()); err != nil {
		replicas := brokerReplicas(c)
		kafka, buildErr := buildKafka(c, replicas)
		if buildErr != nil {
			return fmt.Errorf("build Kafka CR: %w", buildErr)
		}
		if applyErr := c.Apply(kafka); applyErr != nil {
			return fmt.Errorf("create Kafka CR: %w", applyErr)
		}
		l.Info("Kafka CR created", "name", c.Name(), "brokers", replicas)
		return controller.WaitForDuration("waiting for Strimzi operator to provision Kafka cluster", 15*time.Second)
	}

	return waitForKafka(c, existing)
}

// waitForKafka checks the Kafka CR status and returns a WaitError if not yet ready.
func waitForKafka(c *controller.Context, kafka *kafkav1beta2.Kafka) error {
	l := log.FromContext(c.Context())

	if kafka.Status == nil {
		return controller.WaitForDuration("waiting for Strimzi operator to initialize Kafka", 15*time.Second)
	}

	ready, msg := kafkaReadyCondition(kafka)
	if ready {
		l.Info("Kafka cluster is Ready", "name", kafka.Name)
		return nil
	}

	l.Info("Kafka cluster still provisioning", "name", kafka.Name, "message", msg)
	return controller.WaitForDuration(
		fmt.Sprintf("waiting for Strimzi operator to complete Kafka provisioning: %s", msg),
		15*time.Second,
	)
}

// Status reports the current status of the Kafka instance.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	kafka := &kafkav1beta2.Kafka{}
	if err := c.Get(kafka, c.Name()); err != nil {
		return controller.Provisioning("Waiting for Kafka CR"), nil
	}
	if kafka.Status == nil {
		return controller.Provisioning("Waiting for operator to initialize"), nil
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

// Cleanup removes the Kafka CR when the Instance is deleted.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up Kafka instance", "name", c.Name())

	kafka := &kafkav1beta2.Kafka{ObjectMeta: c.ObjectMeta(c.Name())}
	if err := c.Delete(kafka); err != nil {
		return fmt.Errorf("delete Kafka CR: %w", err)
	}

	l.Info("Kafka instance cleaned up", "name", c.Name())
	return nil
}

// =============================================================================
// Builders
// =============================================================================

// buildKafka constructs a Strimzi Kafka CR configured for KRaft mode.
func buildKafka(c *controller.Context, replicas int) (*kafkav1beta2.Kafka, error) {
	engine := c.Instance().Spec.Components[common.ComponentEngine]
	image, err := resolveImage(c, engine)
	if err != nil {
		return nil, err
	}
	kafkaVersion := extractKafkaVersion(image)
	metadataVersion := resolveMetadataVersion(image)
	cpu, memory := resolveResources(engine)
	storageSize, storageClass := resolveStorage(engine)
	kafkaConfig := buildKafkaConfig(replicas)

	replicaCount := int32(replicas) //nolint:gosec
	volID := int32(0)
	deleteClaim := false
	storageSizeStr := storageSize.String()

	// Plain (non-TLS) and TLS internal listeners.
	listenerType := kafkav1beta2.KafkaSpecKafkaListenersElemType("internal")
	listeners := []kafkav1beta2.KafkaSpecKafkaListenersElem{
		{Name: "plain", Port: 9092, Type: listenerType, Tls: false},
		{Name: "tls", Port: 9093, Type: listenerType, Tls: true},
	}

	// JBOD storage with a single persistent volume.
	storageVolume := kafkav1beta2.KafkaSpecKafkaStorageVolumesElem{
		Id:          &volID,
		Type:        kafkav1beta2.KafkaSpecKafkaStorageVolumesElemType("persistent-claim"),
		Size:        &storageSizeStr,
		DeleteClaim: &deleteClaim,
		Class:       storageClass,
	}
	storage := &kafkav1beta2.KafkaSpecKafkaStorage{
		Type:    kafkav1beta2.KafkaSpecKafkaStorageType("jbod"),
		Volumes: []kafkav1beta2.KafkaSpecKafkaStorageVolumesElem{storageVolume},
	}

	// Resources as apiextensions JSON (Strimzi uses freeform resource maps).
	resources := buildResourcesJSON(cpu, memory)

	kafka := &kafkav1beta2.Kafka{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.Name(),
			Namespace: c.Namespace(),
			Annotations: map[string]string{
				// Enable KRaft mode — no ZooKeeper required.
				"strimzi.io/kraft":      "enabled",
				"strimzi.io/node-pools": "enabled",
			},
		},
		Spec: &kafkav1beta2.KafkaSpec{
			Kafka: kafkav1beta2.KafkaSpecKafka{
				Version:         &kafkaVersion,
				MetadataVersion: &metadataVersion,
				Image:           &image,
				Replicas:        &replicaCount,
				Listeners:       listeners,
				Config:          kafkaConfig,
				Storage:         storage,
				Resources:       resources,
			},
			EntityOperator: &kafkav1beta2.KafkaSpecEntityOperator{
				TopicOperator: &kafkav1beta2.KafkaSpecEntityOperatorTopicOperator{},
				UserOperator:  &kafkav1beta2.KafkaSpecEntityOperatorUserOperator{},
			},
		},
	}

	return kafka, nil
}

// buildKafkaConfig returns Kafka broker config scaled to the replica count.
// Replication factors are capped at 3 even if more brokers are requested.
func buildKafkaConfig(replicas int) *apiextensionsv1.JSON {
	rf := replicas
	if rf > 3 {
		rf = 3
	}
	minISR := rf - 1
	if minISR < 1 {
		minISR = 1
	}

	cfg := map[string]string{
		"offsets.topic.replication.factor":         fmt.Sprintf("%d", rf),
		"transaction.state.log.replication.factor": fmt.Sprintf("%d", rf),
		"transaction.state.log.min.isr":            fmt.Sprintf("%d", minISR),
		"default.replication.factor":               fmt.Sprintf("%d", rf),
		"min.insync.replicas":                      fmt.Sprintf("%d", minISR),
	}

	raw, _ := json.Marshal(cfg)
	return &apiextensionsv1.JSON{Raw: raw}
}

// buildResourcesJSON serialises CPU/memory into the freeform JSON format
// that Strimzi's KafkaSpecKafkaResources expects.
func buildResourcesJSON(cpu, memory resource.Quantity) *kafkav1beta2.KafkaSpecKafkaResources {
	cpuStr := cpu.String()
	memStr := memory.String()

	reqRaw, _ := json.Marshal(map[string]string{"cpu": cpuStr, "memory": memStr})
	limRaw, _ := json.Marshal(map[string]string{"cpu": cpuStr, "memory": memStr})

	return &kafkav1beta2.KafkaSpecKafkaResources{
		Requests: &apiextensionsv1.JSON{Raw: reqRaw},
		Limits:   &apiextensionsv1.JSON{Raw: limRaw},
	}
}

// =============================================================================
// Status helpers
// =============================================================================

// kafkaReadyCondition inspects the Kafka status conditions for the Ready condition.
// Returns (true, "") when ready, or (false, message) when not.
func kafkaReadyCondition(kafka *kafkav1beta2.Kafka) (bool, string) {
	if kafka.Status == nil {
		return false, "waiting for status"
	}
	for _, cond := range kafka.Status.Conditions {
		if cond.Type == nil || *cond.Type != "Ready" {
			continue
		}
		if cond.Status != nil && *cond.Status == "True" {
			return true, ""
		}
		msg := ""
		if cond.Message != nil {
			msg = *cond.Message
		}
		return false, msg
	}
	return false, "Ready condition not yet reported"
}

// isKafkaFailed returns true if the Ready condition is False with an Error reason.
func isKafkaFailed(kafka *kafkav1beta2.Kafka) bool {
	if kafka.Status == nil {
		return false
	}
	for _, cond := range kafka.Status.Conditions {
		if cond.Type == nil || cond.Status == nil {
			continue
		}
		if *cond.Type == "Ready" && *cond.Status == "False" {
			if cond.Reason != nil && strings.Contains(*cond.Reason, "Error") {
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
// e.g. "quay.io/strimzi/kafka:0.44.0-kafka-3.9.0" → "3.9.0"
func extractKafkaVersion(image string) string {
	parts := strings.Split(image, "-kafka-")
	if len(parts) == 2 {
		return parts[1]
	}
	// Fallback: use the image tag after the last colon.
	if idx := strings.LastIndex(image, ":"); idx >= 0 {
		return image[idx+1:]
	}
	return "3.9.0"
}

// resolveMetadataVersion derives the KRaft metadata version from the image tag.
func resolveMetadataVersion(image string) string {
	switch {
	case strings.Contains(image, "4.0"):
		return common.KafkaMetadataVersion4_0
	case strings.Contains(image, "3.9"):
		return common.KafkaMetadataVersion3_9
	case strings.Contains(image, "3.8"):
		return common.KafkaMetadataVersion3_8
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
