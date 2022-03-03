package redpanda

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// preCheckConfigurationChange verifies and marks the cluster as needing synchronization (using the ClusterConfigured condition).
// When this method returns true, the cluster CR is marked, indicating that the target cluster will be eventually synchronized (via syncConfiguration).
func (r *ClusterReconciler) markConfigurationChanged(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	configMapResource *resources.ConfigMapResource,
	log logr.Logger,
) (bool, error) {
	if redpandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType) != corev1.ConditionFalse {
		// If the condition is not present, or it does not currently indicate a change, we check for drifts
		if drift, err := configMapResource.CheckCentralizedConfigurationDrift(ctx); err != nil {
			return false, fmt.Errorf("error while checking centralized configuration drift: %w", err)
		} else if drift {
			log.Info("Detected configuration drift in the cluster")

			// Update configuration keys
			_, present, err := configMapResource.GetLastAppliedConfiguration(ctx)
			if err != nil {
				return false, err
			}
			if !present {
				// If no previous annotation was set for last applied configuration keys, then the ones in boostrap.yaml are taken as reference
				// TODO consider conversion from old format to handle the upgrade case, where bootstrap.yaml is not present
				config, err := configMapResource.GetCurrentGlobalConfigurationFromCluster(ctx, configuration.DefaultCentralizedMode())
				if err != nil {
					return false, err
				} else if config != nil {
					if err := configMapResource.SetLastAppliedConfiguration(ctx, config.ClusterConfiguration); err != nil {
						return false, err
					}
				}
			}

			// We need to mark the cluster as changed to trigger the configuration workflow
			redpandaCluster.Status.SetCondition(
				redpandav1alpha1.ClusterConfiguredConditionType,
				corev1.ConditionFalse,
				redpandav1alpha1.ClusterConfiguredReasonUpdating,
				"Detected cluster configuration change that needs to be applied to the cluster",
			)
			return true, r.Status().Update(ctx, redpandaCluster)
		}
	}
	return false, nil
}

// syncConfiguration ensures that the cluster configuration is synchronized with expected data
func (r *ClusterReconciler) syncConfiguration(
	ctx context.Context,
	redpandaCluster *redpandav1alpha1.Cluster,
	configMapResource *resources.ConfigMapResource,
	fqdn string,
	log logr.Logger,
) error {
	if !featuregates.CentralizedConfiguration(redpandaCluster.Spec.Version) {
		log.Info("Cluster is not using centralized configuration, skipping...")
		return nil
	}

	if condition := redpandaCluster.Status.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType); condition == nil {
		// nil condition means no drift detected earlier: we assume configuration is in sync and signal it
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

	if redpandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType) != corev1.ConditionFalse {
		log.Info("Cluster configuration is synchronized")
		return nil
	}

	config, err := configMapResource.CreateConfiguration(ctx)
	if err != nil {
		return fmt.Errorf("error while producing the configuration for cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}
	// TODO wait for the service to be ready before connecting to the admin API

	adminAPI, err := r.getAdminAPIClient(redpandaCluster, fqdn)
	if err != nil {
		return fmt.Errorf("error connecting to the admin API of cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}
	schema, err := adminAPI.ClusterConfigSchema()
	if err != nil {
		return fmt.Errorf("could not get centralized configuration schema from cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}
	clusterConfig, err := adminAPI.Config()
	if err != nil {
		return fmt.Errorf("could not get current centralized configuration from cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}

	lastAppliedConfiguration, _, err := configMapResource.GetLastAppliedConfiguration(ctx)
	if err != nil {
		return err
	}

	patch := configuration.ThreeWayMerge(config.ClusterConfiguration, clusterConfig, lastAppliedConfiguration)
	if !patch.Empty() {
		log.Info("Applying patch to the cluster", "patch", patch.String())
		_, err := adminAPI.PatchClusterConfig(patch.Upsert, patch.Remove)
		if err != nil {
			return fmt.Errorf("could not patch centralized configuration on cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
		}
	}

	// TODO a failure and restart here may lead to inconsistency if the user changes the CR in the meantime (e.g. removing a field),
	// since we applied a config to the cluster but did not store the information anywhere else.
	// A possible fix is doing a two-phase commit (first stage commit on configmap, then apply it to the cluster, with possibility to recover on failure),
	// but it seems overkill given that the case is rare and requires cooperation from the user.

	hash, err := config.GetHash(schema)
	if err != nil {
		return fmt.Errorf("could not compute hash of the new configuration for cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
	}

	stsKey := types.NamespacedName{Name: redpandaCluster.Name, Namespace: redpandaCluster.Namespace}
	sts := appsv1.StatefulSet{}
	if err := r.Get(ctx, stsKey, &sts); err != nil {
		return fmt.Errorf("could not get statefulset %v: %w", stsKey, err)
	}

	oldHash := sts.Annotations[resources.ConfigMapHashAnnotationKey]
	if oldHash == "" {
		// Annotation not yet set on the statefulset (e.g. first time we change config).
		// We check a diff against last applied configuration.
		prevConfig := *config
		prevConfig.ClusterConfiguration = lastAppliedConfiguration
		oldHash, err = prevConfig.GetHash(schema)
		if err != nil {
			return err
		}
	}

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

	// Mark the new lastAppliedConfiguration for next update
	sameConfig := (len(lastAppliedConfiguration) == 0 && len(config.ClusterConfiguration) == 0) || reflect.DeepEqual(lastAppliedConfiguration, config.ClusterConfiguration)
	if !sameConfig {
		if err := configMapResource.SetLastAppliedConfiguration(ctx, config.ClusterConfiguration); err != nil {
			return fmt.Errorf("could not store last applied configuration in the cluster: %w", err)
		}
	}

	// Check status using admin API
	status, err := adminAPI.ClusterConfigStatus()
	if err != nil {
		return fmt.Errorf("could not get config status from admin API")
	}
	originalClusterStatus := redpandaCluster.Status.DeepCopy()
	newCondition := mapToCondition(status)
	redpandaCluster.Status.SetCondition(newCondition.Type, newCondition.Status, newCondition.Reason, newCondition.Message)
	if !reflect.DeepEqual(originalClusterStatus.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType), redpandaCluster.Status.GetCondition(redpandav1alpha1.ClusterConfiguredConditionType)) {
		if err := r.Status().Update(ctx, redpandaCluster); err != nil {
			return fmt.Errorf("could not update condition on cluster %s/%s: %w", redpandaCluster.Namespace, redpandaCluster.Name, err)
		}
		return nil
	}

	// If condition is not met, we need to reschedule.
	// An "Error" reason is not recoverable without changes to the CR, so we don't reschedule in that case.
	if newCondition.Status != corev1.ConditionTrue && newCondition.Reason != redpandav1alpha1.ClusterConfiguredReasonError {
		return &resources.RequeueAfterError{
			RequeueAfter: resources.RequeueDuration,
			Msg:          fmt.Sprintf("cluster configuration is not in sync (%s): %s", newCondition.Reason, newCondition.Message),
		}
	}
	return nil
}

func mapToCondition(
	clusterStatus admin.ConfigStatusResponse,
) redpandav1alpha1.ClusterCondition {
	var condition *redpandav1alpha1.ClusterCondition
	var configVersion int64
	for _, nodeStatus := range clusterStatus {
		if len(nodeStatus.Invalid) > 0 {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonError,
				Message: fmt.Sprintf("Invalid value provided for properties: %s", strings.Join(nodeStatus.Invalid, ", ")),
			}
		} else if len(nodeStatus.Unknown) > 0 {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonError,
				Message: fmt.Sprintf("Unknown properties: %s", strings.Join(nodeStatus.Unknown, ", ")),
			}
		} else if nodeStatus.Restart {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonUpdating,
				Message: fmt.Sprintf("Node %d needs restart", nodeStatus.NodeId),
			}
		} else if configVersion != 0 && nodeStatus.ConfigVersion != configVersion {
			condition = &redpandav1alpha1.ClusterCondition{
				Type:    redpandav1alpha1.ClusterConfiguredConditionType,
				Status:  corev1.ConditionFalse,
				Reason:  redpandav1alpha1.ClusterConfiguredReasonUpdating,
				Message: fmt.Sprintf("Not all nodes share the same configuration version: %d / %d", nodeStatus.ConfigVersion, configVersion),
			}
		}

		configVersion = nodeStatus.ConfigVersion
	}

	if condition == nil {
		// Everything is ok
		condition = &redpandav1alpha1.ClusterCondition{
			Type:   redpandav1alpha1.ClusterConfiguredConditionType,
			Status: corev1.ConditionTrue,
		}
	}
	return *condition
}
