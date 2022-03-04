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

// GetCentralizedConfigurationHash computes a hash of the centralized configuration considering only the
// cluster properties that require a restart (this is why the schema is needed).
// We assume that properties not in schema require restart.
func (c *GlobalConfiguration) GetCentralizedConfigurationHash(
	schema admin.ConfigSchema,
) (string, error) {
	clone := *c

	// Ignore cluster properties that don't require restart
	clone.ClusterConfiguration = make(map[string]interface{})
	for k, v := range c.ClusterConfiguration {
		if meta, ok := schema[k]; !ok || meta.NeedsRestart {
			clone.ClusterConfiguration[k] = v
		}
	}

	serialized, err := clone.Serialize()
	if err != nil {
		return "", err
	}

	// We keep using md5 for having the same format as node hash
	md5Hash := md5.Sum(serialized.BootstrapFile) // nolint:gosec // this is not encrypting secure info
	return fmt.Sprintf("%x", md5Hash), nil
}

func (c *GlobalConfiguration) Convert(targetMode GlobalConfigurationMode) {
	props := make(map[string]interface{}, len(c.ClusterConfiguration)+len(c.NodeConfiguration.Redpanda.Other))
	for k, v := range c.ClusterConfiguration {
		props[k] = v
	}
	for k, v := range c.NodeConfiguration.Redpanda.Other {
		props[k] = v
	}
	c.ClusterConfiguration = nil
	c.NodeConfiguration.Redpanda.Other = nil
	for k, v := range props {
		targetMode.SetAdditionalRedpandaProperty(c, k, v)
	}
}
