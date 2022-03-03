package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigMode(t *testing.T) {
	config := For("v22.1.1-test")
	assert.Equal(t, config.Mode, DefaultCentralizedMode())
	config = For("v21.1.1-test")
	assert.Equal(t, config.Mode, GlobalConfigurationModeClassic)
}

func TestRedpandaProperties(t *testing.T) {
	config := GlobalConfiguration{Mode: GlobalConfigurationModeCentralized}
	config.SetAdditionalRedpandaProperty("a", "b")
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
	assert.NotContains(t, config.NodeConfiguration.Redpanda.Other, "a")

	config = GlobalConfiguration{Mode: GlobalConfigurationModeClassic}
	config.SetAdditionalRedpandaProperty("a", "b")
	assert.NotContains(t, config.ClusterConfiguration, "a")
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])

	config = GlobalConfiguration{Mode: GlobalConfigurationModeMixed}
	config.SetAdditionalRedpandaProperty("a", "b")
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])
}

func TestFlatProperties(t *testing.T) {
	config := GlobalConfiguration{Mode: GlobalConfigurationModeCentralized}
	err := config.SetAdditionalFlatProperties(map[string]string{"redpanda.a": "b", "node_uuid": "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
	assert.Equal(t, "uuid", config.NodeConfiguration.NodeUuid)
	assert.NotContains(t, config.NodeConfiguration.Redpanda.Other, "a")

	config = GlobalConfiguration{Mode: GlobalConfigurationModeClassic}
	err = config.SetAdditionalFlatProperties(map[string]string{"redpanda.a": "b", "node_uuid": "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", config.NodeConfiguration.NodeUuid)
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])
	assert.NotContains(t, config.ClusterConfiguration, "a")

	config = GlobalConfiguration{Mode: GlobalConfigurationModeMixed}
	err = config.SetAdditionalFlatProperties(map[string]string{"redpanda.a": "b", "node_uuid": "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", config.NodeConfiguration.NodeUuid)
	assert.Equal(t, "b", config.NodeConfiguration.Redpanda.Other["a"])
	assert.Equal(t, "b", config.ClusterConfiguration["a"])
}
