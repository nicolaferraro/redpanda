package redpanda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

var _ = Describe("RedPandaCluster configuration controller", func() {

	const (
		timeout  = time.Second * 30
		interval = time.Second * 1

		timeoutShort  = time.Millisecond * 100
		intervalShort = time.Millisecond * 20

		bootstrapConfigurationFile = "bootstrap.yaml"

		configMapHashKey                = "redpanda.vectorized.io/configmap-hash"
		centralizedConfigurationHashKey = "redpanda.vectorized.io/centralized-configuration-hash"
		lastAppliedConfiguraitonHashKey = "redpanda.vectorized.io/last-applied-configuration"
	)

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
			Expect(cm.Annotations[lastAppliedConfiguraitonHashKey]).To(BeEmpty())

			By("Creating the statefulset")
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &appsv1.StatefulSet{})).Should(Succeed())

			By("Setting the configmap-hash annotation on the statefulset")
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(BeEmpty())

			By("Not patching the admin API for any reason")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not using the centralized-config annotation at this stage on the statefulset")
			Eventually(annotationGetter(key, &sts, centralizedConfigurationHashKey), timeout, interval).Should(BeEmpty())
		})

		It("Should interact with the admin API when doing changes", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-changes")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Setting a configmap-hash in the statefulset")
			var sts appsv1.StatefulSet
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(BeEmpty())
			configMapHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(configMapHash).NotTo(BeEmpty())

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
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not changing the configmap-hash in the statefulset")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash))

			By("Not setting the centralized configuration hash in the statefulset")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())

			By("Marking the last applied configuration in the configmap")
			baseConfig, err := testAdminAPI.Config()
			Expect(err).To(BeNil())
			expectedAnnotation, err := json.Marshal(baseConfig)
			Expect(err).To(BeNil())
			Eventually(annotationGetter(baseKey, &corev1.ConfigMap{}, lastAppliedConfiguraitonHashKey), timeout, interval).Should(Equal(string(expectedAnnotation)))
			Consistently(annotationGetter(baseKey, &corev1.ConfigMap{}, lastAppliedConfiguraitonHashKey), timeoutShort, intervalShort).Should(Equal(string(expectedAnnotation)))

			By("Never restarting the cluster")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())
		})

		It("Should remove properties from the admin API when needed", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-removal")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			hash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(hash).NotTo(BeEmpty())

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
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

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
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

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
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Never restarting the cluster")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(hash))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())
		})

		It("Should restart the cluster only when strictly required", func() {

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("central-restart")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Starting with an empty hash")
			var sts appsv1.StatefulSet
			Expect(annotationGetter(key, &sts, centralizedConfigurationHashKey)()).To(BeEmpty())
			initialConfigMapHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(initialConfigMapHash).NotTo(BeEmpty())

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
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Changing the statefulset hash and keeping it stable")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeout, interval).ShouldNot(BeEmpty())
			initialCentralizedConfigHash := annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(Equal(initialCentralizedConfigHash))
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()).To(Equal(initialConfigMapHash))

			By("Accepting another change that would not require restart")
			testAdminAPI.RegisterPropertySchema("prop-no-restart", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			cluster.Spec.AdditionalConfiguration["redpanda.prop-no-restart"] = "the-value2"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the new field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("prop-no-restart"), timeout, interval).Should(Equal("the-value2"))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not changing the hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(Equal(initialCentralizedConfigHash))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(Equal(initialCentralizedConfigHash))
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()).To(Equal(initialConfigMapHash))

			numberOfPatches := len(testAdminAPI.PatchesGetter()())

			By("Accepting a change in a node property to trigger restart")
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			cluster.Spec.Configuration.DeveloperMode = !cluster.Spec.Configuration.DeveloperMode
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Changing the hash because of the node property change")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeout, interval).ShouldNot(Equal(initialConfigMapHash))
			configMapHash2 := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash2))

			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(Equal(initialCentralizedConfigHash))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not patching the admin API for node property changes")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(numberOfPatches))
		})

		It("Should defer updating the centralized configuration when admin API is unavailable", func() {

			testAdminAPI.SetUnavailable(true)

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster := getInitialTestCluster("admin-unavailable")
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the StatefulSet and the ConfigMap")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Setting the configured condition to true on first creation")
			Eventually(clusterConfiguredConditionGetter(key), timeout, interval).ShouldNot(BeNil())
			// Even if the admin API is not available, the first time the cluster is created there's no need to check it
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeTrue())

			By("Accepting a change to the properties")
			testAdminAPI.RegisterPropertySchema("prop", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(k8sClient.Get(context.Background(), key, redpandaCluster)).To(Succeed())
			if redpandaCluster.Spec.AdditionalConfiguration == nil {
				redpandaCluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			redpandaCluster.Spec.AdditionalConfiguration["redpanda.prop"] = "the-value"
			Expect(k8sClient.Update(context.Background(), redpandaCluster)).To(Succeed())

			By("Setting the configured condition to false")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeFalse())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeFalse())

			By("Recovering when the API becomes available again")
			testAdminAPI.SetUnavailable(false)
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())
			Expect(testAdminAPI.PropertyGetter("prop")()).To(Equal("the-value"))
		})

	})

	Context("When reconciling a cluster without centralized configuration", func() {

		It("Should behave like before", func() {

			By("Allowing creation of a cluster with an old version")
			key, baseKey, redpandaCluster := getInitialTestCluster("no-central")
			redpandaCluster.Spec.Version = "v21.11.1-dev" // feature gate disables centralized config but enable shadow index
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap without bootstrap.yaml and the statefulset")
			var cm corev1.ConfigMap
			Eventually(resourceGetter(baseKey, &cm), timeout, interval).Should(Succeed())
			Expect(cm.Data[bootstrapConfigurationFile]).To(BeEmpty())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Not using the condition")
			Consistently(clusterConfiguredConditionGetter(key), timeoutShort, intervalShort).Should(BeNil())

			By("Using the hash annotation directly")
			hash := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Expect(hash).NotTo(BeEmpty())
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(hash))

			By("Not using the central config hash annotation")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())

			By("Not patching the admin API")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))

			By("Accepting a change to any property")
			Expect(k8sClient.Get(context.Background(), key, redpandaCluster)).To(Succeed())
			if redpandaCluster.Spec.AdditionalConfiguration == nil {
				redpandaCluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			redpandaCluster.Spec.AdditionalConfiguration["redpanda.x"] = "any-val"
			Expect(k8sClient.Update(context.Background(), redpandaCluster)).To(Succeed())

			By("Changing the hash annotation again and not the other one")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeout, interval).ShouldNot(Equal(hash))
			hash2 := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Expect(hash2).NotTo(BeEmpty())
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(hash2))
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())
		})

	})

	Context("When upgrading a cluster from a version without centralized configuration", func() {

		It("Should do a single rolling upgrade", func() {

			By("Allowing creation of a cluster with an old version")
			key, baseKey, redpandaCluster := getInitialTestCluster("upgrading")
			redpandaCluster.Spec.Version = "v21.11.1-dev" // feature gate disables centralized config but enables shadow index
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			initialHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(initialHash).NotTo(BeEmpty())
			imageExtractor := func() interface{} {
				conts := make([]string, 0)
				for _, c := range sts.Spec.Template.Spec.Containers {
					conts = append(conts, c.Image)
				}
				return strings.Join(conts, ",")
			}
			initialImages := imageExtractor()

			// By default we set the following properties and they'll be loaded by redpanda from the bootstrap.yaml
			// So we initialize the test admin API with those
			testAdminAPI.SetProperty("auto_create_topics_enabled", false)
			testAdminAPI.SetProperty("cloud_storage_segment_max_upload_interval_sec", 1800)
			testAdminAPI.SetProperty("enable_idempotence", true)
			testAdminAPI.SetProperty("enable_transactions", true)

			By("Accepting the upgrade")
			Expect(resourceGetter(key, redpandaCluster)()).To(Succeed())
			redpandaCluster.Spec.Version = "v22.1.1-dev"
			Expect(k8sClient.Update(context.Background(), redpandaCluster)).To(Succeed())

			By("Changing the Configmap and the statefulset accordingly in one shot")
			var cm corev1.ConfigMap
			Eventually(resourceDataGetter(baseKey, &cm, func() interface{} {
				return cm.Data[bootstrapConfigurationFile]
			}), timeout, interval).ShouldNot(BeEmpty())
			Eventually(resourceDataGetter(key, &sts, imageExtractor), timeout, interval).ShouldNot(Equal(initialImages))
			newConfigMapHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(newConfigMapHash).NotTo(Equal(initialHash)) // If we still set some cluster properties by default, this is expected to happen
			newCentralizedConfigHash := sts.Spec.Template.Annotations[centralizedConfigurationHashKey]
			Expect(newCentralizedConfigHash).To(BeEmpty())
			Consistently(annotationGetter(key, &sts, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())
			Expect(annotationGetter(key, &sts, configMapHashKey)()).To(Equal(newConfigMapHash))

			By("Not patching the admin API for any reason now")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))
		})

		It("Should be able to upgrade and change a cluster even if the admin API is unavailable", func() {

			testAdminAPI.SetUnavailable(true)

			By("Allowing creation of a cluster with an old version")
			key, baseKey, redpandaCluster := getInitialTestCluster("upgrading-unavailable")
			redpandaCluster.Spec.Version = "v21.11.1-dev" // feature gate disables centralized config but enables shadow index
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			initialHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(initialHash).NotTo(BeEmpty())

			By("Accepting the upgrade")
			Expect(resourceGetter(key, redpandaCluster)()).To(Succeed())
			redpandaCluster.Spec.Version = "v22.1.1-dev"
			Expect(k8sClient.Update(context.Background(), redpandaCluster)).To(Succeed())

			By("Changing the Configmap and the statefulset accordingly")
			var cm corev1.ConfigMap
			Eventually(resourceDataGetter(baseKey, &cm, func() interface{} {
				return cm.Data[bootstrapConfigurationFile]
			}), timeout, interval).ShouldNot(BeEmpty())
			// If we still set some cluster properties by default, this is expected to happen
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(Equal(initialHash))
			configMapHash2 := annotationGetter(key, &sts, configMapHashKey)()
			Consistently(annotationGetter(key, &sts, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash2))

			By("Accepting a change in both node and cluster properties")
			testAdminAPI.RegisterPropertySchema("prop", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(resourceGetter(key, redpandaCluster)()).To(Succeed())
			redpandaCluster.Spec.Configuration.DeveloperMode = !redpandaCluster.Spec.Configuration.DeveloperMode
			if redpandaCluster.Spec.AdditionalConfiguration == nil {
				redpandaCluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			redpandaCluster.Spec.AdditionalConfiguration["redpanda.prop"] = "the-value"
			Expect(k8sClient.Update(context.Background(), redpandaCluster)).To(Succeed())

			By("Redeploying the statefulset with the new changes")
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(Equal(configMapHash2))
			configMapHash3 := annotationGetter(key, &sts, configMapHashKey)()
			Consistently(annotationGetter(key, &sts, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash3))

			By("Reflect the problem in the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeFalse())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeFalse())

			By("Recovering when the admin API is available again")
			testAdminAPI.SetUnavailable(false)
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeTrue())

			By("Sending a single patch to the admin API")
			Eventually(testAdminAPI.NumPatchesGetter(), timeout, interval).Should(Equal(1))
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(1))
			Expect(testAdminAPI.PropertyGetter("prop")()).To(Equal("the-value"))

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
			Image:    "vectorized/redpanda",
			Version:  "v22.1.1-dev",
			Replicas: pointer.Int32Ptr(1),
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI: []v1alpha1.KafkaAPI{
					{
						Port: 9092,
					},
				},
				AdminAPI: []v1alpha1.AdminAPI{{Port: 9644}},
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

func resourceDataGetter(key client.ObjectKey, resource client.Object, extractor func() interface{}) func() interface{} {
	return func() interface{} {
		err := resourceGetter(key, resource)()
		if err != nil {
			return err
		}
		return extractor()
	}
}

func annotationGetter(key client.ObjectKey, resource client.Object, name string) func() string {
	return func() string {
		if err := resourceGetter(key, resource)(); err != nil {
			return fmt.Sprintf("client error: %+v", err)
		}
		if sts, ok := resource.(*appsv1.StatefulSet); ok {
			return sts.Spec.Template.Annotations[name]
		}
		return resource.GetAnnotations()[name]
	}
}

func clusterConfiguredConditionGetter(key client.ObjectKey) func() *v1alpha1.ClusterCondition {
	return func() *v1alpha1.ClusterCondition {
		var cluster v1alpha1.Cluster
		if err := k8sClient.Get(context.Background(), key, &cluster); err != nil {
			return nil
		}
		return cluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
	}
}

func clusterConfiguredConditionStatusGetter(key client.ObjectKey) func() bool {
	return func() bool {
		cond := clusterConfiguredConditionGetter(key)()
		return cond != nil && cond.Status == corev1.ConditionTrue
	}
}
