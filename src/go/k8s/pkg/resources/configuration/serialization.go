package configuration

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type SerializedRedpandaConfiguration struct {
	RedpandaFile  []byte
	BootstrapFile []byte
}

func (c *GlobalConfiguration) Serialize() (
	*SerializedRedpandaConfiguration,
	error,
) {
	res := SerializedRedpandaConfiguration{}

	rpConfig, err := yaml.Marshal(c.NodeConfiguration)
	if err != nil {
		return nil, fmt.Errorf("could not serialize node config: %w", err)
	}
	res.RedpandaFile = rpConfig

	if len(c.ClusterConfiguration) > 0 {
		clusterConfig, err := yaml.Marshal(c.ClusterConfiguration)
		if err != nil {
			return nil, fmt.Errorf("could not serialize cluster config: %w", err)
		}
		res.BootstrapFile = clusterConfig
	}
	return &res, nil
}

func (s *SerializedRedpandaConfiguration) Deserialize(
	mode GlobalConfigurationMode,
) (*GlobalConfiguration, error) {
	res := GlobalConfiguration{}
	if s.RedpandaFile != nil {
		if err := yaml.Unmarshal(s.RedpandaFile, &res.NodeConfiguration); err != nil {
			return nil, fmt.Errorf("could not deserialize node config: %w", err)
		}
	}
	if s.BootstrapFile != nil {
		if err := yaml.Unmarshal(s.BootstrapFile, &res.ClusterConfiguration); err != nil {
			return nil, fmt.Errorf("could not deserialize cluster config: %w", err)
		}
	}
	res.Mode = mode // mode is not serialized
	return &res, nil
}
