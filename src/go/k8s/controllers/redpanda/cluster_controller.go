// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package redpanda contains reconciliation logic for redpanda.vectorized.io CRD
package redpanda

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/networking"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/utils"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errNonexistentLastObservesState = errors.New("expecting to have statefulset LastObservedState set but it's nil")
	errNodePortMissing              = errors.New("the node port is missing from the service")
	errInvalidImagePullPolicy       = errors.New("invalid image pull policy")
)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Log                  logr.Logger
	configuratorSettings resources.ConfiguratorSettings
	clusterDomain        string
	Scheme               *runtime.Scheme
}

type configurationPatch struct {
	upsert map[string]interface{}
	remove []string
}

func (p configurationPatch) String() string {
	var buffer bytes.Buffer
	var sep = ""
	for k, v := range p.upsert {
		fmt.Fprint(&buffer, fmt.Sprintf("%s%s=%v", sep, k, v))
		sep = " "
	}
	for _, r := range p.remove {
		fmt.Fprint(&buffer, fmt.Sprintf("%s-%s", sep, r))
		sep = " "
	}
	return buffer.String()
}

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;
//+kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates;clusterissuers,verbs=create;get;list;watch;patch;delete;update;
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=create;get;list;watch;patch;delete;update;
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=create;get;list;watch;patch;delete;update;

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the Cluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
// nolint:funlen // todo break down
func (r *ClusterReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := r.Log.WithValues("redpandacluster", req.NamespacedName)

	log.Info(fmt.Sprintf("Starting reconcile loop for %v", req.NamespacedName))
	defer log.Info(fmt.Sprintf("Finished reconcile loop for %v", req.NamespacedName))

	var redpandaCluster redpandav1alpha1.Cluster
	crb := resources.NewClusterRoleBinding(r.Client, &redpandaCluster, r.Scheme, log)
	if err := r.Get(ctx, req.NamespacedName, &redpandaCluster); err != nil {
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		if apierrors.IsNotFound(err) {
			if removeError := crb.RemoveSubject(ctx, req.NamespacedName); removeError != nil {
				return ctrl.Result{}, fmt.Errorf("unable to remove subject in ClusterroleBinding: %w", removeError)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to retrieve Cluster resource: %w", err)
	}

	managedAnnotationKey := redpandav1alpha1.GroupVersion.Group + "/managed"
	if managed, exists := redpandaCluster.Annotations[managedAnnotationKey]; exists && managed == "false" {
		log.Info(fmt.Sprintf("management of %s is disabled; to enable it, change the '%s' annotation to true or remove it",
			redpandaCluster.Name, managedAnnotationKey))
		return ctrl.Result{}, nil
	}
	redpandaPorts := networking.NewRedpandaPorts(&redpandaCluster)
	nodeports := []resources.NamedServiceNodePort{}
	kafkaAPINamedNodePort := redpandaPorts.KafkaAPI.ToNamedServiceNodePort()
	if kafkaAPINamedNodePort != nil {
		nodeports = append(nodeports, *kafkaAPINamedNodePort)
	}
	adminAPINodePort := redpandaPorts.AdminAPI.ToNamedServiceNodePort()
	if adminAPINodePort != nil {
		nodeports = append(nodeports, *adminAPINodePort)
	}
	pandaProxyNodePort := redpandaPorts.PandaProxy.ToNamedServiceNodePort()
	if pandaProxyNodePort != nil {
		nodeports = append(nodeports, *pandaProxyNodePort)
	}
	schemaRegistryNodePort := redpandaPorts.SchemaRegistry.ToNamedServiceNodePort()
	if schemaRegistryNodePort != nil {
		nodeports = append(nodeports, *schemaRegistryNodePort)
	}
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
	nodeportSvc := resources.NewNodePortService(r.Client, &redpandaCluster, r.Scheme, nodeports, log)

	clusterPorts := []resources.NamedServicePort{}
	if redpandaPorts.PandaProxy.External != nil {
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: resources.PandaproxyPortExternalName, Port: *redpandaPorts.PandaProxy.ExternalPort()})
	}
	if redpandaCluster.Spec.Configuration.SchemaRegistry != nil {
		port := redpandaCluster.Spec.Configuration.SchemaRegistry.Port
		clusterPorts = append(clusterPorts, resources.NamedServicePort{Name: resources.SchemaRegistryPortName, Port: port})
	}
	clusterSvc := resources.NewClusterService(r.Client, &redpandaCluster, r.Scheme, clusterPorts, log)
	subdomain := ""
	proxyAPIExternal := redpandaCluster.PandaproxyAPIExternal()
	if proxyAPIExternal != nil {
		subdomain = proxyAPIExternal.External.Subdomain
	}
	ingress := resources.NewIngress(r.Client,
		&redpandaCluster,
		r.Scheme,
		subdomain,
		clusterSvc.Key().Name,
		resources.PandaproxyPortExternalName,
		log)

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
	pki := certmanager.NewPki(r.Client, &redpandaCluster, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), clusterSvc.ServiceFQDN(r.clusterDomain), r.Scheme, log)
	sa := resources.NewServiceAccount(r.Client, &redpandaCluster, r.Scheme, log)
	configMapResource := resources.NewConfigMap(r.Client, &redpandaCluster, r.Scheme, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), proxySuKey, schemaRegistrySuKey, log)

	// Let's verify if we need to trigger the configuration controller if not already done
	if redpandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType) != corev1.ConditionFalse {
		// Check if configuration changed
		if drift, err := configMapResource.CheckCentralizedConfigurationDrift(ctx); err != nil {
			return ctrl.Result{}, fmt.Errorf("error while checking centralized configuration drift: %w", err)
		} else if drift {
			// We need to mark the cluster as changed to trigger the configuration workflow
			redpandaCluster.Status.SetCondition(
				redpandav1alpha1.ClusterConfiguredConditionType,
				corev1.ConditionFalse,
				redpandav1alpha1.ClusterConfiguredReasonUpdating,
				"Detected cluster configuration change that needs to be applied to the cluster",
			)
			// Changing the status will re-enqueue another request for both controllers
			return ctrl.Result{}, r.Status().Update(ctx, &redpandaCluster)
		}
	}

	sts := resources.NewStatefulSet(
		r.Client,
		&redpandaCluster,
		r.Scheme,
		headlessSvc.HeadlessServiceFQDN(r.clusterDomain),
		headlessSvc.Key().Name,
		nodeportSvc.Key(),
		pki.RedpandaNodeCert(),
		pki.RedpandaOperatorClientCert(),
		pki.RedpandaAdminCert(),
		pki.AdminAPINodeCert(),
		pki.AdminAPIClientCert(),
		pki.PandaproxyAPINodeCert(),
		pki.PandaproxyAPIClientCert(),
		pki.SchemaRegistryAPINodeCert(),
		pki.SchemaRegistryAPIClientCert(),
		sa.Key().Name,
		r.configuratorSettings,
		configMapResource.GetNodeConfigHash,
		log)

	toApply := []resources.Reconciler{
		headlessSvc,
		clusterSvc,
		nodeportSvc,
		ingress,
		proxySu,
		schemaRegistrySu,
		configMapResource,
		pki,
		sa,
		resources.NewClusterRole(r.Client, &redpandaCluster, r.Scheme, log),
		crb,
		resources.NewPDB(r.Client, &redpandaCluster, r.Scheme, log),
		sts,
	}

	for _, res := range toApply {
		err := res.Ensure(ctx)

		var e *resources.RequeueAfterError
		if errors.As(err, &e) {
			log.Error(e, e.Msg)
			return ctrl.Result{RequeueAfter: e.RequeueAfter}, nil
		}

		if err != nil {
			log.Error(err, "Failed to reconcile resource")
			return ctrl.Result{}, err
		}
	}

	var secrets []types.NamespacedName
	if proxySu != nil {
		secrets = append(secrets, proxySu.Key())
	}
	if schemaRegistrySu != nil {
		secrets = append(secrets, schemaRegistrySu.Key())
	}

	err := r.setInitialSuperUserPassword(ctx, &redpandaCluster, headlessSvc.HeadlessServiceFQDN(r.clusterDomain), secrets)

	var e *resources.RequeueAfterError
	if errors.As(err, &e) {
		log.Info(e.Error())
		return ctrl.Result{RequeueAfter: e.RequeueAfter}, nil
	}

	if err != nil {
		log.Error(err, "Failed to set up initial super user password")
		return ctrl.Result{}, err
	}

	schemaRegistryPort := config.DefaultSchemaRegPort
	if redpandaCluster.Spec.Configuration.SchemaRegistry != nil {
		schemaRegistryPort = redpandaCluster.Spec.Configuration.SchemaRegistry.Port
	}
	err = r.reportStatus(
		ctx,
		&redpandaCluster,
		sts.LastObservedState,
		headlessSvc.HeadlessServiceFQDN(r.clusterDomain),
		clusterSvc.ServiceFQDN(r.clusterDomain),
		schemaRegistryPort,
		nodeportSvc.Key(),
	)
	if err != nil {
		log.Error(err, "Unable to report status")
	}

	err = r.SyncCentralizedConfig(
		ctx,
		&redpandaCluster,
		configMapResource,
		headlessSvc.HeadlessServiceFQDN(r.clusterDomain),
		log,
	)
	return ctrl.Result{}, err
}

func (r *ClusterReconciler) SyncCentralizedConfig(ctx context.Context, redpandaCluster *redpandav1alpha1.Cluster, configMapResource *resources.ConfigMapResource, fqdn string, log logr.Logger) error {
	if !featuregates.CentralizedConfiguration(redpandaCluster.Spec.Version) {
		log.Info("Cluster is not using centralized configuration, skipping...")
		return nil
	}

	if redpandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType) != corev1.ConditionFalse {
		log.Info("Cluster configuration is synchronized")
		return nil
	}

	config, err := configMapResource.CreateConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("error while producing the configuration for cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}
	// TODO wait for the service to be ready before connecting to the admin API

	adminAPI, err := utils.NewInternalAdminAPI(redpandaCluster, fqdn)
	if err != nil {
		return fmt.Errorf("error connecting to the admin API of cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}
	schema, err := adminAPI.ClusterConfigSchema()
	if err != nil {
		return fmt.Errorf("could not get centralized configuration schema from cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}

	clusterConfig, err := adminAPI.Config() // TODO use ClusterConfig when available
	if err != nil {
		return fmt.Errorf("could not get current centralized configuration from cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}

	lastUsedKeys, err := configMapResource.GetLastUsedConfigurationKeys(ctx)
	if err != nil {
		return err
	}
	// TODO here we might want to augment and patch the list with property keys we're going to apply, to avoid any kind of data loss

	patch := computePatch(config.ClusterConfiguration, clusterConfig, lastUsedKeys)
	if len(patch.upsert) > 0 || len(patch.remove) > 0 {
		log.Info("Applying patch to the cluster", "patch", patch)
		_, err := adminAPI.PatchClusterConfig(patch.upsert, patch.remove)
		if err != nil {
			return fmt.Errorf("could not patch centralized configuration on cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
		}
	}

	// Mark the new keys for next update
	newKeys := config.GetClusterConfigurationKeys()
	if !reflect.DeepEqual(lastUsedKeys, newKeys) {
		if err := configMapResource.SetLastUsedConfigurationKeys(ctx, newKeys); err != nil {
			return fmt.Errorf("could not mark configuration keys sent to cluster: %w", err)
		}
	}

	filterRestart := filterRestartKeys(schema, config.ClusterConfiguration)
	hash, err := config.GetHash(filterRestart)
	if err != nil {
		return fmt.Errorf("could not compute hash of the new configuration for cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}

	stsKey := types.NamespacedName{Name: redpandaCluster.Name, Namespace: redpandaCluster.Namespace}
	sts := appsv1.StatefulSet{}
	if err := r.Get(ctx, stsKey, &sts); err != nil {
		return fmt.Errorf("could not get statefulset %v: %w", stsKey, err)
	}

	oldHash := sts.Annotations[resources.ConfigMapHashAnnotationKey]
	if oldHash != hash {
		// Needs restart
		if sts.Annotations == nil {
			sts.Annotations = make(map[string]string)
		}
		sts.Annotations[resources.ConfigMapHashAnnotationKey] = hash
		// ignoring banzaicloud last modified annotation on purpose
		if err := r.Update(ctx, &sts); err != nil {
			return fmt.Errorf("could not update config hash on statefulset %v: %w", stsKey, err)
		}
	}

	// Finally update the condition
	redpandaCluster.Status.SetCondition(
		redpandav1alpha1.ClusterConfiguredConditionType,
		corev1.ConditionTrue,
		"", "",
	)
	if err := r.Status().Update(ctx, redpandaCluster); err != nil {
		return fmt.Errorf("could not update condition on cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}

	return nil
}

func computePatch(current, old map[string]interface{}, lastUsedKeys []string) configurationPatch {
	patch := configurationPatch{
		// Initialize them early since nil values are rejected by the server
		upsert: make(map[string]interface{}),
		remove: make([]string, 0),
	}
	for k, v := range current {
		if oldValue, ok := old[k]; !ok || !valueEquals(v, oldValue) {
			patch.upsert[k] = v
		}
	}
	for _, k := range lastUsedKeys {
		if _, ok := current[k]; !ok {
			patch.remove = append(patch.remove, k)
		}
	}
	return patch
}

func valueEquals(v1, v2 interface{}) bool {
	// TODO verify if there's a better way
	// Problems are e.g. when unmarshalled from JSON, numeric values become float64, while they are int when computed
	sv1 := fmt.Sprintf("%v", v1)
	sv2 := fmt.Sprintf("%v", v2)
	return sv1 == sv2
}

func filterRestartKeys(schema admin.ConfigSchema, config map[string]interface{}) map[string]bool {
	filter := make(map[string]bool, len(config))
	for k := range config {
		if s, ok := schema[k]; ok {
			if s.NeedsRestart {
				filter[k] = true
			}
		}
	}
	return filter
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := validateImagePullPolicy(r.configuratorSettings.ImagePullPolicy); err != nil {
		return fmt.Errorf("invalid image pull policy \"%s\": %w", r.configuratorSettings.ImagePullPolicy, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

func validateImagePullPolicy(imagePullPolicy corev1.PullPolicy) error {
	switch imagePullPolicy {
	case corev1.PullAlways:
	case corev1.PullIfNotPresent:
	case corev1.PullNever:
	default:
		return fmt.Errorf("available image pull policy: \"%s\", \"%s\" or \"%s\": %w", corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever, errInvalidImagePullPolicy)
	}
	return nil
}

func (r *ClusterReconciler) reportStatus(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	lastObservedSts *appsv1.StatefulSet,
	internalFQDN string,
	clusterFQDN string,
	schemaRegistryPort int,
	nodeportSvcName types.NamespacedName,
) error {
	var observedPods corev1.PodList

	err := r.List(ctx, &observedPods, &client.ListOptions{
		LabelSelector: labels.ForCluster(redpandaCluster).AsClientSelector(),
		Namespace:     redpandaCluster.Namespace,
	})
	if err != nil {
		return fmt.Errorf("unable to fetch PodList resource: %w", err)
	}

	observedNodesInternal := make([]string, 0, len(observedPods.Items))
	// nolint:gocritic // the copies are necessary for further redpandacluster updates
	for _, item := range observedPods.Items {
		observedNodesInternal = append(observedNodesInternal, fmt.Sprintf("%s.%s", item.Name, internalFQDN))
	}

	nodeList, err := r.createExternalNodesList(ctx, observedPods.Items, redpandaCluster, nodeportSvcName)
	if err != nil {
		return fmt.Errorf("failed to construct external node list: %w", err)
	}

	if lastObservedSts == nil {
		return errNonexistentLastObservesState
	}

	if nodeList == nil {
		nodeList = &redpandav1alpha1.NodesList{
			SchemaRegistry: &redpandav1alpha1.SchemaRegistryStatus{},
		}
	}
	nodeList.Internal = observedNodesInternal
	nodeList.SchemaRegistry.Internal = fmt.Sprintf("%s:%d", clusterFQDN, schemaRegistryPort)

	if statusShouldBeUpdated(&redpandaCluster.Status, nodeList, lastObservedSts.Status.ReadyReplicas) {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var cluster redpandav1alpha1.Cluster
			err := r.Get(ctx, types.NamespacedName{
				Name:      redpandaCluster.Name,
				Namespace: redpandaCluster.Namespace,
			}, &cluster)
			if err != nil {
				return err
			}

			cluster.Status.Nodes = *nodeList
			cluster.Status.Replicas = lastObservedSts.Status.ReadyReplicas

			return r.Status().Update(ctx, &cluster)
		})
		if err != nil {
			return fmt.Errorf("failed to update cluster status: %w", err)
		}
	}
	return nil
}

func statusShouldBeUpdated(
	status *redpandav1alpha1.ClusterStatus,
	nodeList *redpandav1alpha1.NodesList,
	readyReplicas int32,
) bool {
	return nodeList != nil &&
		(!reflect.DeepEqual(nodeList.Internal, status.Nodes.Internal) ||
			!reflect.DeepEqual(nodeList.External, status.Nodes.External) ||
			!reflect.DeepEqual(nodeList.ExternalAdmin, status.Nodes.ExternalAdmin) ||
			!reflect.DeepEqual(nodeList.ExternalPandaproxy, status.Nodes.ExternalPandaproxy) ||
			!reflect.DeepEqual(nodeList.SchemaRegistry, status.Nodes.SchemaRegistry)) ||
		status.Replicas != readyReplicas
}

// WithConfiguratorSettings set the configurator image settings
func (r *ClusterReconciler) WithConfiguratorSettings(
	configuratorSettings resources.ConfiguratorSettings,
) *ClusterReconciler {
	r.configuratorSettings = configuratorSettings
	return r
}

// WithClusterDomain set the clusterDomain
func (r *ClusterReconciler) WithClusterDomain(
	clusterDomain string,
) *ClusterReconciler {
	r.clusterDomain = clusterDomain
	return r
}

// nolint:funlen,gocyclo // External nodes list should be refactored
func (r *ClusterReconciler) createExternalNodesList(
	ctx context.Context,
	pods []corev1.Pod,
	pandaCluster *redpandav1alpha1.Cluster,
	nodePortName types.NamespacedName,
) (*redpandav1alpha1.NodesList, error) {
	externalKafkaListener := pandaCluster.ExternalListener()
	externalAdminListener := pandaCluster.AdminAPIExternal()
	externalProxyListener := pandaCluster.PandaproxyAPIExternal()
	schemaRegistryConf := pandaCluster.Spec.Configuration.SchemaRegistry
	if externalKafkaListener == nil && externalAdminListener == nil && externalProxyListener == nil &&
		(schemaRegistryConf == nil || !pandaCluster.IsSchemaRegistryExternallyAvailable()) {
		return nil, nil
	}

	var nodePortSvc corev1.Service
	if err := r.Get(ctx, nodePortName, &nodePortSvc); err != nil {
		return nil, fmt.Errorf("failed to retrieve node port service %s: %w", nodePortName, err)
	}

	for _, port := range nodePortSvc.Spec.Ports {
		if port.NodePort == 0 {
			return nil, fmt.Errorf("node port service %s, port %s is 0: %w", nodePortName, port.Name, errNodePortMissing)
		}
	}

	var node corev1.Node
	result := &redpandav1alpha1.NodesList{
		External:           make([]string, 0, len(pods)),
		ExternalAdmin:      make([]string, 0, len(pods)),
		ExternalPandaproxy: make([]string, 0, len(pods)),
		SchemaRegistry: &redpandav1alpha1.SchemaRegistryStatus{
			Internal:        "",
			External:        "",
			ExternalNodeIPs: make([]string, 0, len(pods)),
		},
	}

	for i := range pods {
		prefixLen := len(pods[i].GenerateName)
		podName := pods[i].Name[prefixLen:]

		if externalKafkaListener != nil && needExternalIP(externalKafkaListener.External) ||
			externalAdminListener != nil && needExternalIP(externalAdminListener.External) ||
			externalProxyListener != nil && needExternalIP(externalProxyListener.External) ||
			schemaRegistryConf != nil && schemaRegistryConf.External != nil && needExternalIP(*schemaRegistryConf.External) {
			if err := r.Get(ctx, types.NamespacedName{Name: pods[i].Spec.NodeName}, &node); err != nil {
				return nil, fmt.Errorf("failed to retrieve node %s: %w", pods[i].Spec.NodeName, err)
			}
		}

		if externalKafkaListener != nil && len(externalKafkaListener.External.Subdomain) > 0 {
			address := subdomainAddress(podName, externalKafkaListener.External.Subdomain, getNodePort(&nodePortSvc, resources.ExternalListenerName))
			result.External = append(result.External, address)
		} else if externalKafkaListener != nil {
			result.External = append(result.External,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.ExternalListenerName),
				))
		}

		if externalAdminListener != nil && len(externalAdminListener.External.Subdomain) > 0 {
			address := subdomainAddress(podName, externalAdminListener.External.Subdomain, getNodePort(&nodePortSvc, resources.AdminPortExternalName))
			result.ExternalAdmin = append(result.ExternalAdmin, address)
		} else if externalAdminListener != nil {
			result.ExternalAdmin = append(result.ExternalAdmin,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.AdminPortExternalName),
				))
		}

		if externalProxyListener != nil && len(externalProxyListener.External.Subdomain) > 0 {
			address := subdomainAddress(podName, externalProxyListener.External.Subdomain, getNodePort(&nodePortSvc, resources.PandaproxyPortExternalName))
			result.ExternalPandaproxy = append(result.ExternalPandaproxy, address)
		} else if externalProxyListener != nil {
			result.ExternalPandaproxy = append(result.ExternalPandaproxy,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.PandaproxyPortExternalName),
				))
		}

		if schemaRegistryConf != nil && schemaRegistryConf.External != nil && needExternalIP(*schemaRegistryConf.External) {
			result.SchemaRegistry.ExternalNodeIPs = append(result.SchemaRegistry.ExternalNodeIPs,
				fmt.Sprintf("%s:%d",
					getExternalIP(&node),
					getNodePort(&nodePortSvc, resources.SchemaRegistryPortName),
				))
		}
	}

	if schemaRegistryConf != nil && schemaRegistryConf.External != nil && len(schemaRegistryConf.External.Subdomain) > 0 {
		result.SchemaRegistry.External = fmt.Sprintf("%s:%d",
			schemaRegistryConf.External.Subdomain,
			getNodePort(&nodePortSvc, resources.SchemaRegistryPortName),
		)
	}

	if externalProxyListener != nil && len(externalProxyListener.External.Subdomain) > 0 {
		result.PandaproxyIngress = &externalProxyListener.External.Subdomain
	}

	return result, nil
}

func (r *ClusterReconciler) setInitialSuperUserPassword(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	objs []types.NamespacedName,
) error {
	adminAPI, err := utils.NewInternalAdminAPI(redpandaCluster, fqdn)
	if err != nil && errors.Is(err, &utils.NoInternalAdminAPI{}) {
		// TODO maybe this is not equivalent to previous behavior
		return nil
	} else if err != nil {
		return err
	}

	for _, obj := range objs {
		var secret corev1.Secret
		err = r.Get(ctx, types.NamespacedName{
			Namespace: obj.Namespace,
			Name:      obj.Name,
		}, &secret)
		if err != nil {
			return fmt.Errorf("fetching Secret (%s) from namespace (%s): %w", obj.Name, obj.Namespace, err)
		}

		username := string(secret.Data[corev1.BasicAuthUsernameKey])
		password := string(secret.Data[corev1.BasicAuthPasswordKey])

		err = adminAPI.CreateUser(username, password, admin.ScramSha256)
		// {"message": "Creating user: User already exists", "code": 400}
		if err != nil && !strings.Contains(err.Error(), "already exists") { // TODO if user already exists, we only receive "400". Check for specific error code when available.
			return &resources.RequeueAfterError{
				RequeueAfter: resources.RequeueDuration,
				Msg:          fmt.Sprintf("could not create user: %v", err),
			}
		}
	}
	return nil
}

func needExternalIP(external redpandav1alpha1.ExternalConnectivityConfig) bool {
	return external.Subdomain == ""
}

func subdomainAddress(name, subdomain string, port int32) string {
	return fmt.Sprintf("%s.%s:%d",
		name,
		subdomain,
		port,
	)
}

func getExternalIP(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeExternalIP {
			return address.Address
		}
	}
	return ""
}

func getNodePort(svc *corev1.Service, name string) int32 {
	if svc == nil {
		return -1
	}
	for _, port := range svc.Spec.Ports {
		if port.Name == name && port.NodePort != 0 {
			return port.NodePort
		}
	}
	return 0
}
