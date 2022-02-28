package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPatchComputation(t *testing.T) {
	currObj := corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: "res",
			Annotations: map[string]string{
				"redpanda.vectorized.io/configmap-hash": "6fb7ad08cabb116f7c1c8dc1127997a0",
				"c":                                     "d",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: "Always",
		},
	}
	modObj := currObj.DeepCopy()
	modObj.Annotations["e"] = "f"

	current, err := json.Marshal(currObj)
	require.NoError(t, err)
	modified, err := json.Marshal(modObj)
	require.NoError(t, err)

	opt := IgnoreAnnotation("redpanda.vectorized.io/configmap-hash")
	current, modified, err = opt(current, modified)
	currObj2 := corev1.Pod{}
	err = json.Unmarshal(current, &currObj2)
	require.NoError(t, err)
	modObj2 := corev1.Pod{}
	err = json.Unmarshal(modified, &modObj2)
	assert.Empty(t, currObj2.Annotations["redpanda.vectorized.io/configmap-hash"])
	assert.Equal(t, currObj2.Annotations["c"], "d")
	assert.Empty(t, currObj2.Annotations["e"])
	assert.Len(t, currObj2.Annotations, 1)
	assert.Empty(t, modObj2.Annotations["redpanda.vectorized.io/configmap-hash"])
	assert.Equal(t, modObj2.Annotations["c"], "d")
	assert.Equal(t, modObj2.Annotations["e"], "f")
	assert.Len(t, modObj2.Annotations, 2)
}
