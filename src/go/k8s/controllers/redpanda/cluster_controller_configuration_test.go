package redpanda_test

import (
	"context"

	. "github.com/onsi/ginkgo"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"

	//	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

const (
	bootstrapConfigurationFile    = "bootstrap.yaml"
	centralizedConfigContainerTag = "v22.1.1-dev"
)

var _ = Describe("RedPandaCluster configuration controller", func() {

	Context("When creating a RedpandaCluster with centralized config", func() {
		It("Should initialize the cluster", func() {

			resources := corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			}

			key := types.NamespacedName{
				Name:      "internal-redpanda",
				Namespace: "default",
			}
			baseKey := types.NamespacedName{
				Name:      key.Name + "-base",
				Namespace: "default",
			}

			redpandaCluster := &v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      key.Name,
					Namespace: key.Namespace,
				},
				Spec: v1alpha1.ClusterSpec{
					Image:    redpandaContainerImage,
					Version:  centralizedConfigContainerTag,
					Replicas: pointer.Int32Ptr(replicas),
					Configuration: v1alpha1.RedpandaConfig{
						KafkaAPI: []v1alpha1.KafkaAPI{
							{
								Port: kafkaPort,
							},
						},
						AdminAPI: []v1alpha1.AdminAPI{{Port: adminPort}},
					},
					Resources: v1alpha1.RedpandaResourceRequirements{
						ResourceRequirements: corev1.ResourceRequirements{
							Limits:   resources,
							Requests: resources,
						},
						Redpanda: nil,
					},
				},
			}
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating a Configmap with the bootstrap configuration")
			var cm corev1.ConfigMap
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), baseKey, &cm)
				if err != nil {
					return false
				}
				_, exist := cm.Data[bootstrapConfigurationFile]
				return exist
			}, timeout, interval).Should(BeTrue())

		})
	})

})
