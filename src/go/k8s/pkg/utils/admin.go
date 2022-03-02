package utils

import (
	"fmt"

	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
)

type NoInternalAdminAPI struct{}

func (n *NoInternalAdminAPI) Error() string {
	return "no internal admin API defined for cluster"
}

func NewInternalAdminAPI(
	redpandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
) (AdminAPIClient, error) {
	adminInternal := redpandaCluster.AdminAPIInternal()
	if adminInternal == nil {
		return nil, &NoInternalAdminAPI{}
	}

	adminInternalPort := adminInternal.Port

	var urls []string
	replicas := *redpandaCluster.Spec.Replicas

	for i := int32(0); i < replicas; i++ {
		urls = append(urls, fmt.Sprintf("%s-%d.%s:%d", redpandaCluster.Name, i, fqdn, adminInternalPort))
	}

	adminAPI, err := admin.NewAdminAPI(urls, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating admin api: %w", err)
	}
	return adminAPI, nil
}

// AdminAPIClient is a sub interface of the admin API containing what we need in the operator
type AdminAPIClient interface {
	Config() (admin.Config, error)
	ClusterConfigStatus() (admin.ConfigStatusResponse, error)
	ClusterConfigSchema() (admin.ConfigSchema, error)
	PatchClusterConfig(upsert map[string]interface{}, remove []string) (admin.ClusterConfigWriteResult, error)

	CreateUser(username, password, mechanism string) error
}

var _ AdminAPIClient = &admin.AdminAPI{}

type AdminAPIClientFactory func(redpandaCluster *redpandav1alpha1.Cluster, fqdn string) (AdminAPIClient, error)

var _ AdminAPIClientFactory = NewInternalAdminAPI
