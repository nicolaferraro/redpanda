package v1alpha1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestConditions(t *testing.T) {
	cluster := &Cluster{}
	cluster.Status.SetCondition(ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message")
	require.Len(t, cluster.Status.Conditions, 1)
	cond := cluster.Status.Conditions[0]
	assert.Equal(t, ClusterConfiguredConditionType, cond.Type)
	assert.Equal(t, corev1.ConditionTrue, cond.Status)
	assert.Equal(t, "reason", cond.Reason)
	assert.Equal(t, "message", cond.Message)
	condTime := cond.LastTransitionTime
	assert.True(t, condTime.Add(-1*time.Millisecond).Before(time.Now()))

	time.Sleep(10 * time.Millisecond)
	cluster.Status.SetCondition(ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message")
	require.Len(t, cluster.Status.Conditions, 1)
	cond2 := cluster.Status.Conditions[0]
	assert.Equal(t, condTime, cond2.LastTransitionTime)

	condType2 := ClusterConditionType("second")
	cluster.Status.SetCondition(condType2, corev1.ConditionFalse, "reason2", "message2")
	require.Len(t, cluster.Status.Conditions, 2)

	cluster.Status.SetCondition(ClusterConfiguredConditionType, corev1.ConditionUnknown, "reason3", "message3")
	require.Len(t, cluster.Status.Conditions, 2)
	cond3 := cluster.Status.Conditions[0]
	assert.Equal(t, ClusterConfiguredConditionType, cond3.Type)
	assert.Equal(t, corev1.ConditionUnknown, cond3.Status)
	assert.Equal(t, "reason3", cond3.Reason)
	assert.Equal(t, "message3", cond3.Message)
	assert.True(t, cond3.LastTransitionTime.After(condTime.Time))

	condSec := cluster.Status.Conditions[1]
	assert.Equal(t, condType2, condSec.Type)
	assert.Equal(t, corev1.ConditionFalse, condSec.Status)
	assert.Equal(t, "reason2", condSec.Reason)
	assert.Equal(t, "message2", condSec.Message)
}
