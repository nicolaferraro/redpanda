package configuration

import (
	"crypto/md5"
	"fmt"

	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

const (
	// useMixedConfiguration can be temporarily used until .boostrap.yaml is fully supported.
	useMixedConfiguration = true
)

type GlobalConfiguration struct {
	NodeConfiguration    config.Config
	ClusterConfiguration map[string]interface{}
	Mode                 GlobalConfigurationMode
}

func For(version string) *GlobalConfiguration {
	if featuregates.CentralizedConfiguration(version) {
		return &GlobalConfiguration{
			Mode: DefaultCentralizedMode(),
		}
	}
	// Use classic also when version is not present for some reason
	return &GlobalConfiguration{
		Mode: GlobalConfigurationModeClassic,
	}
}

func DefaultCentralizedMode() GlobalConfigurationMode {
	// Use mixed config temporarily
	if useMixedConfiguration {
		return GlobalConfigurationModeMixed
	}
	return GlobalConfigurationModeCentralized
}

func (c *GlobalConfiguration) SetAdditionalRedpandaProperty(
	key string, value interface{},
) {
	c.Mode.SetAdditionalRedpandaProperty(c, key, value)
}

func (c *GlobalConfiguration) SetAdditionalFlatProperties(
	props map[string]string,
) error {
	return c.Mode.SetAdditionalFlatProperties(c, props)
}

// GetHash computes a hash of the configuration considering all node properties and only the cluster properties
// that require a restart (this is why the schema is needed).
// We assume that properties not in schema require restart.
func (c *GlobalConfiguration) GetHash(
	schema admin.ConfigSchema,
) (string, error) {
	clone := *c

	// Ignore cluster properties that don't need restart
	clone.ClusterConfiguration = make(map[string]interface{})
	for k, v := range c.ClusterConfiguration {
		if meta, ok := schema[k]; !ok || meta.NeedsRestart {
			clone.ClusterConfiguration[k] = v
		}
	}

	// Ignore redpanda additional properties that don't need restart
	clone.NodeConfiguration.Redpanda.Other = make(map[string]interface{})
	for k, v := range c.NodeConfiguration.Redpanda.Other {
		if meta, ok := schema[k]; !ok || meta.NeedsRestart {
			clone.NodeConfiguration.Redpanda.Other[k] = v
		}
	}

	serialized, err := clone.Serialize()
	if err != nil {
		return "", err
	}

	full := append([]byte{}, serialized.RedpandaFile...)
	full = append(full, serialized.BootstrapFile...)
	// We keep using md5 for compatibility with previous approach
	md5Hash := md5.Sum(full) // nolint:gosec // this is not encrypting secure info
	return fmt.Sprintf("%x", md5Hash), nil
}
