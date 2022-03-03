package configuration

import (
	"fmt"
	"sort"
	"strings"
)

// CentralConfigurationPatch represents a patch for the redpanda admin API
type CentralConfigurationPatch struct {
	Upsert map[string]interface{}
	Remove []string
}

// String gives a concise representation of the patch
func (p CentralConfigurationPatch) String() string {
	upsert := make([]string, 0, len(p.Upsert))
	for k, v := range p.Upsert {
		upsert = append(upsert, fmt.Sprintf("%s=%v", k, v))
	}
	remove := make([]string, 0, len(p.Remove))
	for _, r := range p.Remove {
		remove = append(remove, fmt.Sprintf("-%s", r))
	}
	sort.Strings(upsert)
	sort.Strings(remove)
	return strings.Join(append(upsert, remove...), " ")
}

func (p CentralConfigurationPatch) Empty() bool {
	return len(p.Upsert) == 0 && len(p.Remove) == 0
}

func ThreeWayMerge(
	apply map[string]interface{},
	current map[string]interface{},
	lastApplied map[string]interface{},
) CentralConfigurationPatch {
	patch := CentralConfigurationPatch{
		// Initialize them early since nil values are rejected by the server
		Upsert: make(map[string]interface{}),
		Remove: make([]string, 0),
	}
	for k, v := range apply {
		if oldValue, ok := current[k]; !ok || !valueEquals(v, oldValue) {
			patch.Upsert[k] = v
		}
	}
	for k := range lastApplied {
		_, isPresent := apply[k]
		_, wasPresent := current[k]
		if !isPresent && wasPresent {
			patch.Remove = append(patch.Remove, k)
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
