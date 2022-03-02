package configuration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputePatch(t *testing.T) {
	tests := []struct {
		name     string
		current  map[string]interface{}
		existing map[string]interface{}
		usedKeys []string
		expected CentralConfigurationPatch
	}{
		{
			name: "simple",
			current: map[string]interface{}{
				"a": "b",
			},
			existing: map[string]interface{}{
				"c": "d",
			},
			usedKeys: []string{},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{},
			},
		},
		{
			name: "remove dangling",
			current: map[string]interface{}{
				"a": "b",
			},
			existing: map[string]interface{}{
				"c": "d",
			},
			usedKeys: []string{"c", "x"},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{"c"},
			},
		},
		{
			name: "upsert mismatches only",
			current: map[string]interface{}{
				"a": "b",
				"c": "x",
			},
			existing: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
			usedKeys: []string{"a", "c"},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"c": "x",
				},
				Remove: []string{},
			},
		},
		{
			name: "support type conversion",
			current: map[string]interface{}{
				"a": "b",
				"c": 23,
			},
			existing: map[string]interface{}{
				"a": "b",
				"c": "23",
			},
			usedKeys: []string{"a", "c"},
			expected: CentralConfigurationPatch{
				Upsert: map[string]interface{}{},
				Remove: []string{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("ComputePatch-%s", tc.name), func(t *testing.T) {
			compPatch := ComputePatch(tc.current, tc.existing, tc.usedKeys)
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
