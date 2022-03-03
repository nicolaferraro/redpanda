package configuration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputePatch(t *testing.T) {
	tests := []struct {
		name        string
		apply       map[string]interface{}
		current     map[string]interface{}
		lastApplied map[string]interface{}
		expected    CentralConfigurationPatch
	}{
		{
			name: "simple",
			apply: map[string]interface{}{
				"a": "b",
			},
			current: map[string]interface{}{
				"c": "d",
			},
			lastApplied: map[string]interface{}{},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{},
			},
		},
		{
			name: "remove dangling",
			apply: map[string]interface{}{
				"a": "b",
			},
			current: map[string]interface{}{
				"c": "d",
			},
			lastApplied: map[string]interface{}{"c": "xx", "x": "xx"},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{"c"},
			},
		},
		{
			name: "upsert mismatches only",
			apply: map[string]interface{}{
				"a": "b",
				"c": "x",
			},
			current: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
			lastApplied: map[string]interface{}{"a": "xx", "c": "xx"},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"c": "x",
				},
				Remove: []string{},
			},
		},
		{
			name: "support type conversion",
			apply: map[string]interface{}{
				"a": "b",
				"c": 23,
			},
			current: map[string]interface{}{
				"a": "b",
				"c": "23",
			},
			lastApplied: map[string]interface{}{"a": "xx", "c": "xx"},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{},
				Remove: []string{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("ComputePatch-%s", tc.name), func(t *testing.T) {
			compPatch := ThreeWayMerge(tc.apply, tc.current, tc.lastApplied)
			assert.Equal(t, tc.expected.Upsert, compPatch.Upsert, "Upsert does not match")
			assert.Equal(t, tc.expected.Remove, compPatch.Remove, "Remove list does not match")
		})
	}
}

func TestString(t *testing.T) {
	p := CentralConfigurationPatch{
		Upsert: map[string]interface{}{"c": "d", "a": "b"},
		Remove: []string{"x", "e", "f"},
	}
	assert.Equal(t, "a=b c=d -e -f -x", p.String())
}
