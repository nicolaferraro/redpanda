package redpanda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	//	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

const (
	bootstrapConfigurationFile    = "bootstrap.yaml"
	centralizedConfigContainerTag = "v22.1.1-dev"

	timeoutShort  = time.Second * 1
	intervalShort = time.Millisecond * 200
)

var _ = Describe("RedPandaCluster configuration controller", func() {

	Context("When managing a RedpandaCluster with centralized config", func() {

		It("Can initialize a cluster with centralized configuration", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-initialize")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating a Configmap with the bootstrap configuration")
			var cm corev1.ConfigMap
			Eventually(resourceGetter(baseKey, &cm), timeout, interval).Should(Succeed())

			By("Putting a bootstrap.yaml in the configmap")
			Expect(cm.Data[bootstrapConfigurationFile]).ToNot(BeEmpty())

			By("Skipping the last-applied-configuration annotation on the configmap")
			Expect(cm.Annotations["redpanda.vectorized.io/last-applied-configuration"]).To(BeEmpty())

			By("Creating the statefulset")
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &appsv1.StatefulSet{})).Should(Succeed())

			By("Skipping the configmap-hash annotation on the statefulset")
			Expect(sts.Annotations["redpanda.vectorized.io/configmap-hash"]).To(BeEmpty())

			By("Not calling the admin API for any reason")
			Consistently(testAdminAPI.IsEmptyGetter(), timeoutShort, intervalShort).Should(BeTrue())

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())
		})

		It("Should interact with the admin API when doing changes", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-changes")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Accepting a change")
			testAdminAPI.RegisterPropertySchema("non-restarting", admin.ConfigPropertyMetadata{NeedsRestart: false})
			var cluster v1alpha1.Cluster
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.non-restarting"] = "the-val"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Sending the new property to the admin API")
			Eventually(testAdminAPI.PropertyGetter("non-restarting"), timeout, interval).Should(Equal("the-val"))
			Consistently(testAdminAPI.PatchesGetter(), timeoutShort, intervalShort).Should(HaveLen(1))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Not changing the configmap-hash in the statefulset")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeoutShort, intervalShort).Should(BeEmpty())

			By("Marking the last applied configuration in the configmap")
			baseConfig, err := testAdminAPI.Config()
			Expect(err).To(BeNil())
			expectedAnnotation, err := json.Marshal(baseConfig)
			Expect(err).To(BeNil())
			Eventually(annotationGetter(baseKey, &corev1.ConfigMap{}, "redpanda.vectorized.io/last-applied-configuration"), timeout, interval).Should(Equal(string(expectedAnnotation)))
			Consistently(annotationGetter(baseKey, &corev1.ConfigMap{}, "redpanda.vectorized.io/last-applied-configuration"), timeoutShort, intervalShort).Should(Equal(string(expectedAnnotation)))

			By("Never restarting the cluster")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeoutShort, intervalShort).Should(BeEmpty())
		})

		It("Should remove properties from the admin API when needed", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-removal")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Accepting an initial change to synchronize")
			testAdminAPI.RegisterPropertySchema("p0", admin.ConfigPropertyMetadata{NeedsRestart: false})
			var cluster v1alpha1.Cluster
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.p0"] = "v0"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("p0"), timeout, interval).Should(Equal("v0"))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting two new properties")
			testAdminAPI.RegisterPropertySchema("p1", admin.ConfigPropertyMetadata{NeedsRestart: false})
			testAdminAPI.RegisterPropertySchema("p2", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.p1"] = "v1"
			cluster.Spec.AdditionalConfiguration["redpanda.p2"] = "v2"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Adding two fields to the config at once")
			Eventually(testAdminAPI.PropertyGetter("p1")).Should(Equal("v1"))
			Eventually(testAdminAPI.PropertyGetter("p2")).Should(Equal("v2"))
			patches := testAdminAPI.PatchesGetter()()
			Expect(patches).NotTo(BeEmpty())
			Expect(patches[len(patches)-1]).To(Equal(configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"p1": "v1",
					"p2": "v2",
				},
				Remove: []string{},
			}))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting a deletion and a change")
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.p1"] = "v1x"
			delete(cluster.Spec.AdditionalConfiguration, "redpanda.p2")
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Producing the right patch")
			Eventually(testAdminAPI.PropertyGetter("p1")).Should(Equal("v1x"))
			patches = testAdminAPI.PatchesGetter()()
			Expect(patches).NotTo(BeEmpty())
			Expect(patches[len(patches)-1]).To(Equal(configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"p1": "v1x",
				},
				Remove: []string{"p2"},
			}))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Never restarting the cluster")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeoutShort, intervalShort).Should(BeEmpty())
		})

		It("Should restart the cluster only when strictly required", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-restart")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Starting with an empty hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash")()).To(BeEmpty())

			By("Accepting a change that would require restart")
			testAdminAPI.RegisterPropertySchema("prop-restart", admin.ConfigPropertyMetadata{NeedsRestart: true})
			var cluster v1alpha1.Cluster
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.prop-restart"] = "the-value"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("prop-restart"), timeout, interval).Should(Equal("the-value"))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Changing the statefulset hash and keeping it stable")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeout, interval).ShouldNot(BeEmpty())
			hash1 := annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash")()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeoutShort, intervalShort).Should(Equal(hash1))

			By("Accepting another change that would not require restart")
			testAdminAPI.RegisterPropertySchema("prop-no-restart", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			cluster.Spec.AdditionalConfiguration["redpanda.prop-no-restart"] = "the-value2"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the new field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("prop-no-restart"), timeout, interval).Should(Equal("the-value2"))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Not changing the hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash")()).To(Equal(hash1))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeoutShort, intervalShort).Should(Equal(hash1))

			numberOfPatches := len(testAdminAPI.PatchesGetter()())

			By("Accepting a change in a node property to trigger restart")
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			cluster.Spec.Configuration.DeveloperMode = !cluster.Spec.Configuration.DeveloperMode
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Changing the hash because of the node property change")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeout, interval).ShouldNot(Equal(hash1))
			hash2 := annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash")()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, "redpanda.vectorized.io/configmap-hash"), timeoutShort, intervalShort).Should(Equal(hash2))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredGetter(key), timeout, interval).Should(BeTrue())

			By("Not calling the admin API for node property changes")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(numberOfPatches))
		})

	})

})

func getInitialTestCluster(name string) (key types.NamespacedName, baseKey types.NamespacedName, cluster *v1alpha1.Cluster) {
	key = types.NamespacedName{
		Name:      name,
		Namespace: "default",
	}
	baseKey = types.NamespacedName{
		Name:      name + "-base",
		Namespace: "default",
	}

	cluster = &v1alpha1.Cluster{
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
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
				Redpanda: nil,
			},
		},
	}
	return
}

func resourceGetter(key client.ObjectKey, resource client.Object) func() error {
	return func() error {
		return k8sClient.Get(context.Background(), key, resource)
	}
}

func annotationGetter(key client.ObjectKey, resource client.Object, name string) func() string {
	return func() string {
		if err := resourceGetter(key, resource)(); err != nil {
			return fmt.Sprintf("client error: %+v", err)
		}
		return resource.GetAnnotations()[name]
	}
}

func clusterConfiguredGetter(key client.ObjectKey) func() bool {
	return func() bool {
		var cluster v1alpha1.Cluster
		if err := k8sClient.Get(context.Background(), key, &cluster); err != nil {
			return false
		}
		return cluster.Status.GetConditionStatus(v1alpha1.ClusterConfiguredConditionType) == corev1.ConditionTrue
	}
}
