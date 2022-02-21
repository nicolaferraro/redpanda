package configuration

import (
	"reflect"

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
	if version != "" && featuregates.CentralizedConfiguration(version) {
		// Use mixed config temporarily
		if useMixedConfiguration {
			return &GlobalConfiguration{
				Mode: GlobalConfigurationModeMixed,
			}
		}
		return &GlobalConfiguration{
			Mode: GlobalConfigurationModeCentralized,
		}
	}
	// Use classic also when version is not present for some reason
	return &GlobalConfiguration{
		Mode: GlobalConfigurationModeClassic,
	}
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

func (c *GlobalConfiguration) Equals(c2 *GlobalConfiguration) bool {
	return reflect.DeepEqual(c, c2)
}
