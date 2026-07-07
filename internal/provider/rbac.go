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

// Package provider defines the provider implementation.
// RBAC markers for the Strimzi Kafka operator resources.
package provider

// OpenEverest core resources reconciled by the provider-runtime.
// These base grants come from the provider-sdk scaffold; without them the
// controller cannot watch/reconcile Instances ("instances.core.openeverest.io
// is forbidden") and no Kafka cluster is ever provisioned. Do not remove.
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.openeverest.io,resources=providers,verbs=get;list;watch

// Strimzi Kafka cluster
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkas/status,verbs=get
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkas/finalizers,verbs=update

// Strimzi KafkaNodePool (KRaft mode)
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkanodepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkanodepools/status,verbs=get

// Strimzi KafkaTopic and KafkaUser (managed by entity operator)
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkatopics,verbs=get;list;watch
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkausers,verbs=get;list;watch

// Core Kubernetes resources managed by the Strimzi operator on our behalf
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
