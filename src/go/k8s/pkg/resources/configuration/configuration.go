package configuration

import (
	"crypto/md5"
	"fmt"
	"sort"

	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/featuregates"
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

func (c *GlobalConfiguration) GetHash(
	filterClusterProps map[string]bool,
) (string, error) {
	clone := *c
	clone.ClusterConfiguration = make(map[string]interface{})
	for k, v := range c.ClusterConfiguration {
		if p, ok := filterClusterProps[k]; ok && p {
			clone.ClusterConfiguration[k] = v
		}
	}

	serialized, err := clone.Serialize()
	if err != nil {
		return "", err
	}

	full := append([]byte{}, serialized.RedpandaFile...)
	full = append(full, serialized.BootstrapFile...)

	md5Hash := md5.Sum(full) // nolint:gosec // this is not encrypting secure info
	return fmt.Sprintf("%x", md5Hash), nil
}

func (c *GlobalConfiguration) GetClusterConfigurationKeys() []string {
	keys := make([]string, 0, len(c.ClusterConfiguration))
	for k := range c.ClusterConfiguration {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
