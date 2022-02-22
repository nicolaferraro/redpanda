// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/networking"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterConfigurationReconciler reconciles the configuration of a cluster object
type ClusterConfigurationReconciler struct {
	client.Client
	Log           logr.Logger
	clusterDomain string
	Scheme        *runtime.Scheme
}

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;

// Reconcile runs a secondary controller on the cluster resource that deals with changing
// configuration on the cluster when using centralized configuration.
func (r *ClusterConfigurationReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := r.Log.WithValues("redpandacluster-configuration", req.NamespacedName)

	log.Info(fmt.Sprintf("Starting configuration reconcile loop for %v", req.NamespacedName))
	defer log.Info(fmt.Sprintf("Finished configuration reconcile loop for %v", req.NamespacedName))

	var redpandaCluster redpandav1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &redpandaCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to retrieve Cluster resource: %w", err)
	}

	if redpandaCluster.Spec.Version == "" || !featuregates.CentralizedConfiguration(redpandaCluster.Spec.Version) {
		// This controller should only watch clusters using the new centralized configuration
		return ctrl.Result{}, nil
	}

	if redpandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType) != corev1.ConditionFalse {
		// We execute the config update workflow only if explicitly triggered via change in the condition
		return ctrl.Result{}, nil
	}

	// TODO deduplicate the code below
	redpandaPorts := networking.NewRedpandaPorts(&redpandaCluster)
	headlessPorts := []resources.NamedServicePort{}
	if redpandaPorts.AdminAPI.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: resources.AdminPortName, Port: *redpandaPorts.AdminAPI.InternalPort()})
	}
	if redpandaPorts.KafkaAPI.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: resources.InternalListenerName, Port: *redpandaPorts.KafkaAPI.InternalPort()})
	}
	if redpandaPorts.PandaProxy.Internal != nil {
		headlessPorts = append(headlessPorts, resources.NamedServicePort{Name: resources.PandaproxyPortInternalName, Port: *redpandaPorts.PandaProxy.InternalPort()})
	}
	headlessSvc := resources.NewHeadlessService(r.Client, &redpandaCluster, r.Scheme, headlessPorts, log)

	var proxySu *resources.SuperUsersResource
	var proxySuKey types.NamespacedName
	if redpandaCluster.Spec.EnableSASL && redpandaCluster.PandaproxyAPIInternal() != nil {
		proxySu = resources.NewSuperUsers(r.Client, &redpandaCluster, r.Scheme, resources.ScramPandaproxyUsername, resources.PandaProxySuffix, log)
		proxySuKey = proxySu.Key()
	}
	var schemaRegistrySu *resources.SuperUsersResource
	var schemaRegistrySuKey types.NamespacedName
	if redpandaCluster.Spec.EnableSASL && redpandaCluster.Spec.Configuration.SchemaRegistry != nil {
		schemaRegistrySu = resources.NewSuperUsers(r.Client, &redpandaCluster, r.Scheme, resources.ScramSchemaRegistryUsername, resources.SchemaRegistrySuffix, log)
		schemaRegistrySuKey = schemaRegistrySu.Key()
	}
	// TODO deduplicate the code above

	configMapResource := resources.NewConfigMap(r.Client, &redpandaCluster, r.Scheme, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), proxySuKey, schemaRegistrySuKey, log)

	config, err := configMapResource.CreateConfiguration(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error while creating the configuration for cluster %v: %w", req.NamespacedName, err)
	}

	adminAPI, err := utils.NewInternalAdminAPI(&redpandaCluster, headlessSvc.HeadlessServiceFQDN(r.clusterDomain))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error connecting to the admin API of cluster %v: %w", req.NamespacedName, err)
	}

	schema, err := adminAPI.ClusterConfigSchema()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get centralized configuration schema from cluster %v: %w", req.NamespacedName, err)
	}

	clusterConfig, err := adminAPI.Config()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get current centralized configuration from cluster %v: %w", req.NamespacedName, err)
	}

	schemaJSON, _ := json.Marshal(schema)
	curr, _ := json.Marshal(config.ClusterConfiguration)
	clus, _ := json.Marshal(clusterConfig)

	fmt.Println("-- schema --")
	fmt.Println(string(schemaJSON))
	fmt.Println("-- current --")
	fmt.Println(string(curr))
	fmt.Println("-- cluster --")
	fmt.Println(string(clus))
	fmt.Println("-- end --")

	// TODO apply the new config
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterConfigurationReconciler) SetupWithManager(
	mgr ctrl.Manager,
) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

// WithClusterDomain set the clusterDomain
func (r *ClusterConfigurationReconciler) WithClusterDomain(
	clusterDomain string,
) *ClusterConfigurationReconciler {
	r.clusterDomain = clusterDomain
	return r
}
