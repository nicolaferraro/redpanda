package configuration

import (
	"strconv"
	"strings"

	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

type GlobalConfigurationMode interface {
	SetAdditionalRedpandaProperty(targetConfig *GlobalConfiguration, key string, value interface{})
	SetAdditionalFlatProperties(targetConfig *GlobalConfiguration, props map[string]string) error
}

var (
	GlobalConfigurationModeClassic     GlobalConfigurationMode = globalConfigurationModeClassic{}
	GlobalConfigurationModeCentralized GlobalConfigurationMode = globalConfigurationModeCentralized{}
	GlobalConfigurationModeMixed       GlobalConfigurationMode = globalConfigurationModeMixed{}
)

const (
	redpandaPropertyPrefix = "redpanda."
)

// globalConfigurationModeClassic provides classic configuration rules
type globalConfigurationModeClassic struct{}

func (r globalConfigurationModeClassic) SetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string, value interface{},
) {
	if targetConfig.NodeConfiguration.Redpanda.Other == nil {
		targetConfig.NodeConfiguration.Redpanda.Other = make(map[string]interface{})
	}
	targetConfig.NodeConfiguration.Redpanda.Other[key] = value
}

func (r globalConfigurationModeClassic) SetAdditionalFlatProperties(
	targetConfig *GlobalConfiguration, props map[string]string,
) error {
	// all properties are node properties in the classic setting
	mgr := config.NewManager(afero.NewOsFs())
	err := mgr.Merge(&targetConfig.NodeConfiguration)
	if err != nil {
		return err
	}

	// Add arbitrary parameters to configuration
	for k, v := range props {
		if builtInType(v) {
			err = mgr.Set(k, v, "single")
			if err != nil {
				return err
			}
		} else {
			err = mgr.Set(k, v, "")
			if err != nil {
				return err
			}
		}
	}

	newRpCfg, err := mgr.Get()
	if err != nil {
		return err
	}
	targetConfig.NodeConfiguration = *newRpCfg
	return nil
}

// globalConfigurationModeCentralized provides centralized configuration rules
type globalConfigurationModeCentralized struct{}

func (r globalConfigurationModeCentralized) SetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string, value interface{},
) {
	if targetConfig.ClusterConfiguration == nil {
		targetConfig.ClusterConfiguration = make(map[string]interface{})
	}
	targetConfig.ClusterConfiguration[key] = value
}

func (r globalConfigurationModeCentralized) SetAdditionalFlatProperties(
	targetConfig *GlobalConfiguration, props map[string]string,
) error {
	remaining := make(map[string]string, len(props))
	for key, value := range props {
		if strings.HasPrefix(key, redpandaPropertyPrefix) {
			// TODO verify if we can assume that all *additional* properties starting with "redpanda." are central properties
			// (i.e. we have a CR mapping for all redpanda node properties)
			newKey := strings.TrimPrefix(key, redpandaPropertyPrefix)
			if targetConfig.ClusterConfiguration == nil {
				targetConfig.ClusterConfiguration = make(map[string]interface{})
			}
			targetConfig.ClusterConfiguration[newKey] = value
		} else {
			remaining[key] = value
		}
	}
	if len(remaining) > 0 {
		return GlobalConfigurationModeClassic.SetAdditionalFlatProperties(targetConfig, remaining)
	}
	return nil
}

// globalConfigurationModeMixed provides mixed configuration rules
type globalConfigurationModeMixed struct{}

func (r globalConfigurationModeMixed) SetAdditionalRedpandaProperty(
	targetConfig *GlobalConfiguration, key string, value interface{},
) {
	GlobalConfigurationModeClassic.SetAdditionalRedpandaProperty(targetConfig, key, value)
	GlobalConfigurationModeCentralized.SetAdditionalRedpandaProperty(targetConfig, key, value)
}

func (r globalConfigurationModeMixed) SetAdditionalFlatProperties(
	targetConfig *GlobalConfiguration, props map[string]string,
) error {
	// We put unknown properties in both buckets e.g. during upgrades
	if err := GlobalConfigurationModeClassic.SetAdditionalFlatProperties(targetConfig, props); err != nil {
		return err
	}
	return GlobalConfigurationModeCentralized.SetAdditionalFlatProperties(targetConfig, props)
}

func builtInType(value string) bool {
	if _, err := strconv.Atoi(value); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseBool(value); err == nil {
		return true
	}
	return false
}
