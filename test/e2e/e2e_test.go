//go:build e2e

// Package e2e asserts that a kcp workspace can carry the storage for a CRD that
// would otherwise live in the workload cluster's own apiserver.
//
// The environment is provisioned by hack/e2e.sh, which exports the kubeconfigs
// and the workspace URL these tests read.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	"k8s.io/utils/ptr"
)

var (
	kcpKubeconfig  = os.Getenv("KCP_KUBECONFIG")
	workspaceURL   = os.Getenv("KCP_WORKSPACE_URL")
	kindKubeconfig = os.Getenv("KIND_KUBECONFIG")
	namespace      = envOrDefault("E2E_NAMESPACE", "default")
	curlImage      = envOrDefault("CURL_IMAGE", "curlimages/curl:8.18.0")
)

const (
	widgetGroup = "spillway.example.com"
	crdName     = "widgets." + widgetGroup
)

var apiServiceGVR = schema.GroupVersionResource{
	Group:    "apiregistration.k8s.io",
	Version:  "v1",
	Resource: "apiservices",
}

const apiServiceName = "v1alpha1." + widgetGroup

var widgetGVR = schema.GroupVersionResource{
	Group:    widgetGroup,
	Version:  "v1alpha1",
	Resource: "widgets",
}

func TestMain(m *testing.M) {
	for name, value := range map[string]string{
		"KCP_KUBECONFIG":    kcpKubeconfig,
		"KCP_WORKSPACE_URL": workspaceURL,
		"KIND_KUBECONFIG":   kindKubeconfig,
	} {
		if value == "" {
			fmt.Fprintf(os.Stderr, "%s is unset -- run 'hack/e2e.sh up' first\n", name)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// kcpConfig returns a client configuration scoped to the seeded workspace.
// rootConfig is the shard-wide credential, with no workspace selected. The
// caller sets Host to whichever workspace it means.
func rootConfig(t *testing.T) *rest.Config {
	t.Helper()

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kcpKubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: "root"},
	).ClientConfig()
	if err != nil {
		t.Fatalf("loading kcp kubeconfig: %v", err)
	}
	return config
}

func kcpConfig(t *testing.T) *rest.Config {
	t.Helper()

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kcpKubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: "root"},
	).ClientConfig()
	if err != nil {
		t.Fatalf("loading kcp kubeconfig: %v", err)
	}

	// The credentials are shard wide; the workspace is selected by URL path.
	config.Host = workspaceURL
	return config
}

func kindConfig(t *testing.T) *rest.Config {
	t.Helper()

	config, err := clientcmd.BuildConfigFromFlags("", kindKubeconfig)
	if err != nil {
		t.Fatalf("loading kind kubeconfig: %v", err)
	}

	// client-go throttles to 5 requests a second by default. These tests poll,
	// and a throttled client reports "client rate limiter Wait returned an
	// error" instead of what the server said -- which reads exactly like the
	// behaviour under test having failed.
	config.QPS = 100
	config.Burst = 200
	return config
}

func testContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// TestWorkspaceServesWidgetAPI checks that the offloaded group is discoverable
// in kcp. Everything downstream depends on this, so it fails loudly and early.
func TestWorkspaceServesWidgetAPI(t *testing.T) {
	client, err := discovery.NewDiscoveryClientForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building discovery client: %v", err)
	}

	groups, err := client.ServerGroups()
	if err != nil {
		t.Fatalf("listing server groups in the workspace: %v", err)
	}

	for _, group := range groups.Groups {
		if group.Name == widgetGroup {
			return
		}
	}
	t.Fatalf("workspace at %s does not serve group %q", workspaceURL, widgetGroup)
}

func TestWidgetCRDIsEstablished(t *testing.T) {
	client, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building apiextensions client: %v", err)
	}

	crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(testContext(t), crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting %s from the workspace: %v", crdName, err)
	}

	for _, condition := range crd.Status.Conditions {
		if condition.Type == apiextensionsv1.Established {
			if condition.Status != apiextensionsv1.ConditionTrue {
				t.Fatalf("%s is not Established: %s: %s", crdName, condition.Reason, condition.Message)
			}
			return
		}
	}
	t.Fatalf("%s has no Established condition", crdName)
}

// TestSeededWidgetsAreStoredInKCP reads back the resources hack/e2e.sh created,
// including the one that relies on the schema default, which only holds if kcp
// really ran the CRD through its own defaulting rather than storing it raw.
func TestSeededWidgetsAreStoredInKCP(t *testing.T) {
	client, err := dynamic.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client: %v", err)
	}

	list, err := client.Resource(widgetGVR).Namespace(namespace).List(testContext(t), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing widgets: %v", err)
	}

	sizes := map[string]int64{}
	colors := map[string]string{}
	for _, item := range list.Items {
		size, found, err := unstructured.NestedInt64(item.Object, "spec", "size")
		if err != nil || !found {
			t.Errorf("widget %s has no spec.size (found=%v, err=%v)", item.GetName(), found, err)
			continue
		}
		color, _, _ := unstructured.NestedString(item.Object, "spec", "color")
		sizes[item.GetName()] = size
		colors[item.GetName()] = color
	}

	for name, want := range map[string]int64{
		"red-widget":   7,
		"blue-widget":  42,
		"green-widget": 1, // omitted in the manifest; supplied by the schema default
	} {
		got, ok := sizes[name]
		if !ok {
			t.Errorf("widget %s was not returned by kcp", name)
			continue
		}
		if got != want {
			t.Errorf("widget %s: spec.size = %d, want %d", name, got, want)
		}
	}

	if colors["red-widget"] != "red" {
		t.Errorf("widget red-widget: spec.color = %q, want %q", colors["red-widget"], "red")
	}
}

// TestWidgetRoundTrip exercises the write path rather than just the seeded
// state: create, update, and delete a resource whose storage is kcp.
func TestWidgetRoundTrip(t *testing.T) {
	ctx := testContext(t)

	client, err := dynamic.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client: %v", err)
	}
	widgets := client.Resource(widgetGVR).Namespace(namespace)

	const name = "roundtrip-widget"
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"color": "blue", "size": int64(3)},
	}}

	created, err := widgets.Create(ctx, widget, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating widget: %v", err)
	}
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	if created.GetUID() == "" {
		t.Error("created widget has no UID, so kcp did not persist it")
	}
	if created.GetResourceVersion() == "" {
		t.Error("created widget has no resourceVersion")
	}

	if err := unstructured.SetNestedField(created.Object, int64(99), "spec", "size"); err != nil {
		t.Fatalf("setting spec.size: %v", err)
	}
	updated, err := widgets.Update(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating widget: %v", err)
	}
	if size, _, _ := unstructured.NestedInt64(updated.Object, "spec", "size"); size != 99 {
		t.Errorf("after update: spec.size = %d, want 99", size)
	}

	// The CRD caps size at 100, so this must be rejected by kcp's validation.
	if err := unstructured.SetNestedField(updated.Object, int64(1000), "spec", "size"); err != nil {
		t.Fatalf("setting spec.size: %v", err)
	}
	if _, err := widgets.Update(ctx, updated, metav1.UpdateOptions{}); err == nil {
		t.Error("expected kcp to reject spec.size=1000, but the update succeeded")
	}

	if err := widgets.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting widget: %v", err)
	}
	if _, err := widgets.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("after delete, Get returned %v, want NotFound", err)
	}
}

// TestWidgetCRDIsAbsentFromWorkloadCluster is the premise of the project. The
// group is reachable in the workload cluster, but only because spillway serves
// it: there is no CRD, and so none of the storage cost, in this cluster.
func TestWidgetCRDIsAbsentFromWorkloadCluster(t *testing.T) {
	apiextensionsClient, err := apiextensionsclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building apiextensions client for the kind cluster: %v", err)
	}

	_, err = apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(testContext(t), crdName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("getting %s from the kind cluster returned %v, want NotFound: the resources are supposed to be stored in kcp", crdName, err)
	}
}

// TestAPIServiceIsAvailable checks the aggregation layer accepted spillway. The
// probe behind this condition is a real request to
// /apis/<group>/<version>, so a false here means discovery is broken rather
// than merely unregistered.
func TestAPIServiceIsAvailable(t *testing.T) {
	client, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client for the kind cluster: %v", err)
	}

	apiService, err := client.Resource(apiServiceGVR).Get(testContext(t), apiServiceName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting APIService %s: %v", apiServiceName, err)
	}

	conditions, _, err := unstructured.NestedSlice(apiService.Object, "status", "conditions")
	if err != nil {
		t.Fatalf("reading APIService conditions: %v", err)
	}
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok || condition["type"] != "Available" {
			continue
		}
		if condition["status"] != "True" {
			t.Fatalf("APIService %s is not Available: %v: %v", apiServiceName, condition["reason"], condition["message"])
		}
		return
	}
	t.Fatalf("APIService %s has no Available condition", apiServiceName)
}

// TestPodsCanReachKCP is the precondition for spillway ever being deployable in
// the cluster: a pod has to be able to open a TLS connection to kcp at the
// address kcp put in its own certificate.
func TestPodsCanReachKCP(t *testing.T) {
	ctx := testContext(t)

	parsed, err := url.Parse(workspaceURL)
	if err != nil {
		t.Fatalf("parsing workspace URL %q: %v", workspaceURL, err)
	}
	target := fmt.Sprintf("%s://%s/livez", parsed.Scheme, parsed.Host)

	client, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building clientset for the kind cluster: %v", err)
	}

	const jobName = "kcp-reachability"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To(int32(3)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "curl",
						Image:           curlImage,
						ImagePullPolicy: corev1.PullNever, // preloaded by hack/e2e.sh
						Command:         []string{"sh", "-c"},
						// A TLS handshake plus any HTTP status proves reachability;
						// an unauthenticated 401 is a perfectly good answer here.
						Args: []string{fmt.Sprintf(
							`code=$(curl -sS -k -o /dev/null -w '%%{http_code}' --max-time 10 %s); `+
								`echo "kcp returned HTTP $code"; [ "$code" != "000" ]`, target)},
					}},
				},
			},
		},
	}

	_ = client.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
	})
	if err := waitForJobGone(ctx, client, jobName); err != nil {
		t.Fatalf("waiting for a previous %s job to disappear: %v", jobName, err)
	}

	if _, err := client.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the reachability job: %v", err)
	}
	t.Cleanup(func() {
		_ = client.BatchV1().Jobs(namespace).Delete(context.Background(), jobName, metav1.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
		})
	})

	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			current, err := client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if current.Status.Succeeded > 0 {
				return true, nil
			}
			if current.Status.Failed > *job.Spec.BackoffLimit {
				return false, fmt.Errorf("job failed %d times: a pod in the cluster cannot reach kcp at %s",
					current.Status.Failed, target)
			}
			return false, nil
		})
	if err != nil {
		t.Fatalf("pods cannot reach kcp at %s: %v", target, err)
	}
}

func waitForJobGone(ctx context.Context, client kubernetes.Interface, name string) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, nil
		})
}

// TestWidgetsThroughAggregationLayer is the assertion this whole harness exists
// to make: a client talking to the workload cluster reads and writes widgets
// that are actually stored in kcp.
func TestWidgetsThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client for the kind cluster: %v", err)
	}
	viaKCP, err := dynamic.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client for kcp: %v", err)
	}

	clusterWidgets := viaCluster.Resource(widgetGVR).Namespace(namespace)
	kcpWidgets := viaKCP.Resource(widgetGVR).Namespace(namespace)

	// Reads through the cluster must return what kcp holds.
	list, err := clusterWidgets.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing widgets through the aggregation layer: %v", err)
	}
	seeded := map[string]int64{}
	for _, item := range list.Items {
		size, _, _ := unstructured.NestedInt64(item.Object, "spec", "size")
		seeded[item.GetName()] = size
	}
	for name, want := range map[string]int64{"red-widget": 7, "blue-widget": 42, "green-widget": 1} {
		if got, found := seeded[name]; !found || got != want {
			t.Errorf("through the aggregation layer, widget %s = %d (found=%v), want %d", name, got, found, want)
		}
	}

	// Writes through the cluster must land in kcp.
	const name = "aggregated-widget"
	created, err := clusterWidgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"color": "red", "size": int64(12)},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a widget through the aggregation layer: %v", err)
	}
	t.Cleanup(func() {
		_ = clusterWidgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	fromKCP, err := kcpWidgets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the widget created through the cluster is not in kcp: %v", err)
	}
	if fromKCP.GetUID() != created.GetUID() {
		t.Errorf("UID in kcp = %s, through the cluster = %s: these should be the same object",
			fromKCP.GetUID(), created.GetUID())
	}
	if size, _, _ := unstructured.NestedInt64(fromKCP.Object, "spec", "size"); size != 12 {
		t.Errorf("spec.size in kcp = %d, want 12", size)
	}

	// And kcp's schema validation still applies to requests that arrive this way.
	if err := unstructured.SetNestedField(created.Object, int64(1000), "spec", "size"); err != nil {
		t.Fatalf("setting spec.size: %v", err)
	}
	if _, err := clusterWidgets.Update(ctx, created, metav1.UpdateOptions{}); err == nil {
		t.Error("expected kcp to reject spec.size=1000 through the aggregation layer, but the update succeeded")
	}

	// Deletes propagate too.
	if err := clusterWidgets.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting through the aggregation layer: %v", err)
	}
	if _, err := kcpWidgets.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("after deleting through the cluster, kcp still has the widget: %v", err)
	}
}

// TestWatchThroughAggregationLayer checks that events stream rather than
// arriving in a lump when the watch closes, which is what a naively buffered
// proxy would do.
func TestWatchThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client for the kind cluster: %v", err)
	}
	widgets := viaCluster.Resource(widgetGVR).Namespace(namespace)

	list, err := widgets.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing widgets: %v", err)
	}

	watcher, err := widgets.Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
	if err != nil {
		t.Fatalf("starting a watch through the aggregation layer: %v", err)
	}
	defer watcher.Stop()

	const name = "watched-widget"
	if _, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"color": "blue"},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating a widget to observe: %v", err)
	}
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	for {
		select {
		case event, open := <-watcher.ResultChan():
			if !open {
				t.Fatal("the watch closed before the event arrived")
			}
			object, ok := event.Object.(*unstructured.Unstructured)
			if !ok || object.GetName() != name {
				continue
			}
			if event.Type != watch.Added {
				t.Errorf("event type = %s, want ADDED", event.Type)
			}
			return
		case <-time.After(30 * time.Second):
			t.Fatal("no watch event arrived within 30s: events are not streaming through the proxy")
		}
	}
}

// TestNewCRDAppearsThroughAggregationLayer is the reason discovery is driven by
// a watch rather than a poll: a CRD added to the workspace has to show up in
// the workload cluster promptly, not after the backstop interval.
func TestNewCRDAppearsThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	kcpClient, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building apiextensions client for kcp: %v", err)
	}

	const (
		plural  = "gadgets"
		crdName = plural + "." + widgetGroup
	)
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: widgetGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind: "Gadget", ListKind: "GadgetList", Plural: plural, Singular: "gadget",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {Type: "object"},
						},
					},
				},
			}},
		},
	}

	if _, err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the CRD in kcp: %v", err)
	}
	t.Cleanup(func() {
		_ = kcpClient.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
	})

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building discovery client for the kind cluster: %v", err)
	}

	// Comfortably inside the backstop, so passing means the watch did it.
	const budget = 90 * time.Second
	started := time.Now()

	err = wait.PollUntilContextTimeout(ctx, time.Second, budget, true,
		func(context.Context) (bool, error) {
			resources, err := discoveryClient.ServerResourcesForGroupVersion(widgetGroup + "/v1alpha1")
			if err != nil {
				// Discovery blips while spillway republishes the group, so this
				// is a reason to keep polling rather than to fail.
				return false, nil //nolint:nilerr // transient by design
			}
			for _, resource := range resources.APIResources {
				if resource.Name == plural {
					return true, nil
				}
			}
			return false, nil
		})
	if err != nil {
		t.Fatalf("%s never appeared in the workload cluster's discovery within %s: %v", plural, budget, err)
	}
	t.Logf("a CRD added to the workspace reached the workload cluster in %s", time.Since(started).Round(time.Second))
}

// TestOwnerOutsideTheWorkspaceIsRefused pins down a failure that used to lose
// data silently. kcp runs its own garbage collector over the workspace, and an
// ownerReference naming something the workspace does not contain looks to it
// exactly like an owner that has been deleted: measured against a real kcp, the
// object was gone in under five seconds, with no event and no trace.
func TestOwnerOutsideTheWorkspaceIsRefused(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building clientset for the kind cluster: %v", err)
	}
	owner, err := cluster.CoreV1().ConfigMaps(namespace).Create(ctx,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owner-outside", Namespace: namespace}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating the owner: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.CoreV1().ConfigMaps(namespace).Delete(context.Background(), owner.Name, metav1.DeleteOptions{})
	})

	widgets, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client: %v", err)
	}

	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name": "owned-by-the-cluster",
			"ownerReferences": []any{map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"name":       owner.Name,
				"uid":        string(owner.UID),
			}},
		},
		"spec": map[string]any{"color": "red"},
	}}

	_, err = widgets.Resource(widgetGVR).Namespace(namespace).Create(ctx, widget, metav1.CreateOptions{})
	if err == nil {
		t.Cleanup(func() {
			_ = widgets.Resource(widgetGVR).Namespace(namespace).
				Delete(context.Background(), "owned-by-the-cluster", metav1.DeleteOptions{})
		})
		t.Fatal("an owner outside the workspace was accepted; kcp will collect this object within seconds")
	}
	if !strings.Contains(err.Error(), "garbage collector") {
		t.Errorf("the rejection does not explain what would have happened: %v", err)
	}
}

// Ownership within the workspace is ordinary: kcp can see the owner, so it
// behaves exactly as it would for any CRD.
func TestOwnerInsideTheWorkspaceIsAccepted(t *testing.T) {
	ctx := testContext(t)

	widgets, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client: %v", err)
	}
	client := widgets.Resource(widgetGVR).Namespace(namespace)

	parent, err := client.Get(ctx, "red-widget", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the owner: %v", err)
	}

	child := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name": "owned-within",
			"ownerReferences": []any{map[string]any{
				"apiVersion": widgetGroup + "/v1alpha1",
				"kind":       "Widget",
				"name":       parent.GetName(),
				"uid":        string(parent.GetUID()),
			}},
		},
		"spec": map[string]any{"color": "blue"},
	}}

	if _, err := client.Create(ctx, child, metav1.CreateOptions{}); err != nil {
		t.Fatalf("an owner inside the workspace was rejected: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Delete(context.Background(), "owned-within", metav1.DeleteOptions{})
	})
}

// TestValidatingWebhookIsCalled proves the webhook bridge exists at all.
//
// A ValidatingWebhookConfiguration in the workload cluster is consumed by that
// cluster's admission chain, which an aggregated API never passes through, so
// before spillway invoked admission itself these webhooks simply did not fire
// for offloaded resources -- silently, which is the dangerous part for anyone
// relying on policy.
//
// The webhook here points at a host that does not resolve. With failurePolicy
// Fail a create must be refused, which can only happen if spillway called it;
// with Ignore the same create must succeed, which shows the refusal came from
// the webhook rather than from anything else in the path.
func TestValidatingWebhookIsCalled(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building clientset for the kind cluster: %v", err)
	}
	widgets, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building dynamic client: %v", err)
	}

	const configName = "spillway-e2e-webhook"
	unreachable := "https://no-such-webhook.invalid/validate"
	sideEffects := admissionregistrationv1.SideEffectClassNone

	webhook := func(policy admissionregistrationv1.FailurePolicyType) *admissionregistrationv1.ValidatingWebhookConfiguration {
		return &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: configName},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{
				Name:                    "widgets.spillway.e2e",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: &unreachable},
				FailurePolicy:           &policy,
				SideEffects:             &sideEffects,
				AdmissionReviewVersions: []string{"v1"},
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{
						admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{widgetGroup},
						APIVersions: []string{"v1alpha1"},
						Resources:   []string{"widgets"},
					},
				}},
			}},
		}
	}

	fail := admissionregistrationv1.Fail
	if _, err := cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().
		Create(ctx, webhook(fail), metav1.CreateOptions{}); err != nil {
		t.Fatalf("registering the webhook: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().
			Delete(context.Background(), configName, metav1.DeleteOptions{})
	})

	newWidget := func(name string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": widgetGroup + "/v1alpha1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"color": "red"},
		}}
	}
	client := widgets.Resource(widgetGVR).Namespace(namespace)

	// Spillway watches the configurations through an informer, so give it a
	// moment to notice this one.
	var lastErr error
	err = wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			created, err := client.Create(ctx, newWidget("webhook-denied"), metav1.CreateOptions{})
			if err == nil {
				_ = client.Delete(ctx, created.GetName(), metav1.DeleteOptions{})
				return false, nil
			}
			lastErr = err
			return strings.Contains(err.Error(), "webhook"), nil
		})
	if err != nil {
		t.Fatalf("a create was never refused by the webhook, so it is not being called (last error: %v)", lastErr)
	}
	t.Logf("the webhook was called: %v", lastErr)

	// A patch must reach the webhook too. kube-apiserver applies the patch and
	// then admits the result, so spillway asks kcp what the patch would produce
	// and admits that -- a webhook that is skipped on patch is a policy that
	// can be walked around with kubectl patch.
	// A patch must reach the webhook too: kube-apiserver applies a patch and
	// then admits the result, so a webhook skipped on patch is a policy that
	// kubectl patch walks around.
	err = wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := client.Patch(ctx, "red-widget", types.MergePatchType,
				[]byte(`{"spec":{"size":5}}`), metav1.PatchOptions{})
			if err == nil {
				return false, nil
			}
			lastErr = err
			return strings.Contains(err.Error(), "webhook"), nil
		})
	if err != nil {
		t.Fatalf("a patch was never refused by the webhook (last error: %v)", lastErr)
	}
	t.Logf("the webhook was called for a patch: %v", lastErr)

	// With Ignore, an unreachable webhook must not block the write.
	ignore := admissionregistrationv1.Ignore
	existing, err := cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().
		Get(ctx, configName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the webhook configuration: %v", err)
	}
	existing.Webhooks[0].FailurePolicy = &ignore
	if _, err := cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().
		Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("relaxing the failure policy: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			created, err := client.Create(ctx, newWidget("webhook-ignored"), metav1.CreateOptions{})
			if err != nil {
				// Still being refused: the relaxed policy has not reached
				// spillway's informer yet, so keep waiting rather than failing.
				return false, nil //nolint:nilerr // waiting for the update to propagate
			}
			t.Cleanup(func() {
				_ = client.Delete(context.Background(), created.GetName(), metav1.DeleteOptions{})
			})
			return true, nil
		})
	if err != nil {
		t.Error("with failurePolicy Ignore an unreachable webhook still blocked the write")
	}
}

// TestOpenAPIV2DescribesOnlyTheOffloadedGroup checks the v2 document spillway
// builds for clients too old to use v3.
//
// It cannot be proxied the way v3 is: kcp's own v2 document describes only its
// built-in types and contains no custom resource at all, so this is built from
// the workspace's CRDs instead. Filtering matters as much as building -- a
// definition merged into the cluster's spec for a type the cluster does not
// serve would be worse than no document.
func TestOpenAPIV2DescribesOnlyTheOffloadedGroup(t *testing.T) {
	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building clientset for the kind cluster: %v", err)
	}

	raw, err := cluster.Discovery().RESTClient().Get().
		AbsPath("/apis/" + widgetGroup + "/v1alpha1").DoRaw(testContext(t))
	if err != nil {
		t.Fatalf("reaching the aggregated group: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("the group answered nothing")
	}

	// The aggregation layer merges spillway's v2 into the cluster's, so ask
	// spillway's own endpoint through the APIService proxy.
	document, err := cluster.Discovery().RESTClient().Get().
		AbsPath("/openapi/v2").DoRaw(testContext(t))
	if err != nil {
		t.Fatalf("reading the cluster's merged OpenAPI v2: %v", err)
	}

	var swagger struct {
		Paths       map[string]any `json:"paths"`
		Definitions map[string]any `json:"definitions"`
	}
	if err := json.Unmarshal(document, &swagger); err != nil {
		t.Fatalf("decoding the merged document: %v", err)
	}

	var described bool
	for path := range swagger.Paths {
		if strings.Contains(path, widgetGroup) {
			described = true
		}
	}
	if !described {
		t.Errorf("the merged v2 document describes no %s path, so clients on v2 cannot see the offloaded API", widgetGroup)
	}
}

// TestNamespaceDeletionReachesKCP asks what happens to offloaded objects when
// the namespace holding them is deleted in the workload cluster.
//
// Nothing in spillway implements this. The cluster's namespace controller
// enumerates every namespaced resource it can discover -- aggregated ones
// included -- and deletes what it finds, so the deletion should travel through
// spillway into kcp on its own. Should is not a guarantee: if it does not, the
// objects outlive the namespace, and whoever is given that namespace name next
// inherits the previous tenant's objects the moment they recreate it.
//
// The same enumeration is why an unreachable spillway leaves namespaces stuck
// Terminating: the controller cannot finish what it cannot list.
func TestNamespaceDeletionReachesKCP(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset for the kind cluster: %v", err)
	}
	workspace, err := kubernetes.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the clientset for kcp: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client for the kind cluster: %v", err)
	}
	viaKCP, err := dynamic.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client for kcp: %v", err)
	}

	// The namespace has to exist in both places: kcp stores the objects, and
	// the cluster's namespace admission runs on the way in.
	const ns = "spillway-cascade"
	for name, client := range map[string]kubernetes.Interface{"kcp": workspace, "the cluster": cluster} {
		if _, err := client.CoreV1().Namespaces().Create(ctx,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
			metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating namespace %s in %s: %v", ns, name, err)
		}
	}
	t.Cleanup(func() {
		_ = workspace.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		_ = cluster.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
	})

	clusterWidgets := viaCluster.Resource(widgetGVR).Namespace(ns)
	kcpWidgets := viaKCP.Resource(widgetGVR).Namespace(ns)

	names := []string{"doomed-one", "doomed-two", "doomed-three"}
	for _, name := range names {
		if _, err := clusterWidgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": widgetGroup + "/v1alpha1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"color": "red", "size": int64(3)},
		}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating widget %s in namespace %s: %v", name, ns, err)
		}
	}

	stored, err := kcpWidgets.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing the widgets in kcp: %v", err)
	}
	if len(stored.Items) != len(names) {
		t.Fatalf("kcp holds %d widgets in %s, want %d", len(stored.Items), ns, len(names))
	}

	if err := cluster.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting namespace %s in the cluster: %v", ns, err)
	}

	// A namespace that never finishes terminating is itself the failure: it
	// means the controller could not account for the offloaded resource.
	terminated := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := cluster.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		})
	if terminated != nil {
		namespace, getErr := cluster.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if getErr == nil {
			t.Errorf("namespace %s is still %s after two minutes, conditions: %+v",
				ns, namespace.Status.Phase, namespace.Status.Conditions)
		}
		t.Fatalf("waiting for namespace %s to terminate: %v", ns, terminated)
	}

	left, err := kcpWidgets.List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing the widgets in kcp after the namespace went away: %v", err)
	}
	if len(left.Items) != 0 {
		surviving := make([]string, 0, len(left.Items))
		for _, item := range left.Items {
			surviving = append(surviving, item.GetName())
		}
		t.Errorf("namespace %s was deleted in the cluster but kcp still holds %d widget(s) in it: %v -- "+
			"recreating the namespace would hand them to whoever gets the name next",
			ns, len(left.Items), surviving)
	}
}

// TestAPIServiceIsRegisteredForANewVersion drives the whole registration
// lifecycle against a real cluster: a version appears in the workspace, an
// APIService for it appears in the cluster and goes Available, the version goes
// away, and the APIService goes with it.
//
// A hand-written APIService cannot do this. The one in config/spillway.yaml
// names v1alpha1 and nothing else, so a workspace that starts serving v1beta1
// tomorrow is invisible until somebody notices and edits a manifest.
func TestAPIServiceIsRegisteredForANewVersion(t *testing.T) {
	ctx := testContext(t)

	kcpClient, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for kcp: %v", err)
	}
	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client for the kind cluster: %v", err)
	}

	const (
		crdName        = "gadgets." + widgetGroup
		apiServiceName = "v1beta1." + widgetGroup
	)

	if _, err := aggregator.ApiregistrationV1().APIServices().Get(ctx, apiServiceName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("APIService %s exists before the version does: %v", apiServiceName, err)
	}

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: widgetGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "gadgets", Singular: "gadget", Kind: "Gadget", ListKind: "GadgetList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1beta1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type:       "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{"teeth": {Type: "integer"}},
							},
						},
					},
				},
			}},
		},
	}
	if _, err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the gadgets CRD in kcp: %v", err)
	}
	removed := false
	t.Cleanup(func() {
		if !removed {
			_ = kcpClient.ApiextensionsV1().CustomResourceDefinitions().
				Delete(context.Background(), crdName, metav1.DeleteOptions{})
		}
		_ = aggregator.ApiregistrationV1().APIServices().
			Delete(context.Background(), apiServiceName, metav1.DeleteOptions{})
	})

	// Spillway watches the workspace's CRDs, so this is bounded by how long kcp
	// takes to establish one rather than by any poll interval.
	var registered *apiregistrationv1.APIService
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			found, err := aggregator.ApiregistrationV1().APIServices().Get(ctx, apiServiceName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			registered = found
			return true, nil
		}); err != nil {
		t.Fatalf("waiting for spillway to register %s: %v", apiServiceName, err)
	}

	if registered.Labels["app.kubernetes.io/managed-by"] != "spillway" {
		t.Errorf("labels on %s = %v, want it marked as spillway's own", apiServiceName, registered.Labels)
	}
	if registered.Spec.Service == nil || registered.Spec.Service.Name != "spillway" {
		t.Errorf("service reference on %s = %+v, want spillway's", apiServiceName, registered.Spec.Service)
	}

	// Registered is not the same as usable: the aggregation layer has to be able
	// to reach spillway on the new version too.
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			found, err := aggregator.ApiregistrationV1().APIServices().Get(ctx, apiServiceName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			for _, condition := range found.Status.Conditions {
				if condition.Type == apiregistrationv1.Available {
					return condition.Status == apiregistrationv1.ConditionTrue, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("waiting for %s to become Available: %v", apiServiceName, err)
	}

	// The APIService spillway did not create must still be there, untouched.
	declared, err := aggregator.ApiregistrationV1().APIServices().Get(ctx, "v1alpha1."+widgetGroup, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the declared APIService: %v", err)
	}
	if _, claimed := declared.Labels["app.kubernetes.io/managed-by"]; claimed {
		t.Errorf("spillway claimed the hand-written APIService: labels = %v", declared.Labels)
	}

	// And when the workspace stops serving the version, the registration goes.
	if err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().
		Delete(ctx, crdName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the gadgets CRD from kcp: %v", err)
	}
	removed = true

	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := aggregator.ApiregistrationV1().APIServices().Get(ctx, apiServiceName, metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		}); err != nil {
		t.Errorf("after the workspace stopped serving v1beta1, %s is still registered: %v", apiServiceName, err)
	}
}

// TestSubresourcesThroughAggregationLayer covers the paths a controller lives
// on. Nothing exercised /status before this: spillway forwards whatever path it
// is given, so a subresource is meant to work by construction -- which is
// exactly the kind of claim that turns out to be wrong.
func TestSubresourcesThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	kcpClient, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for kcp: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client for the kind cluster: %v", err)
	}

	const crdName = "sprockets." + widgetGroup
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: widgetGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "sprockets", Singular: "sprocket", Kind: "Sprocket", ListKind: "SprocketList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.replicas",
						StatusReplicasPath: ".status.replicas",
					},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"replicas": {Type: "integer"},
								},
							},
							"status": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"replicas": {Type: "integer"},
									"phase":    {Type: "string"},
								},
							},
						},
					},
				},
			}},
		},
	}
	if _, err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the sprockets CRD in kcp: %v", err)
	}
	t.Cleanup(func() {
		_ = kcpClient.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
	})

	gvr := schema.GroupVersionResource{Group: widgetGroup, Version: "v1alpha1", Resource: "sprockets"}
	sprockets := viaCluster.Resource(gvr).Namespace(namespace)

	// The resource has to reach the cluster before it can be used, which is the
	// CRD watch doing its job.
	const name = "one"
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := sprockets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": widgetGroup + "/v1alpha1",
				"kind":       "Sprocket",
				"metadata":   map[string]any{"name": name},
				"spec":       map[string]any{"replicas": int64(3)},
			}}, metav1.CreateOptions{})
			return err == nil, nil
		}); err != nil {
		t.Fatalf("waiting for sprockets to be servable through the aggregation layer: %v", err)
	}
	t.Cleanup(func() {
		_ = sprockets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	// A write to the main resource must not be able to set status, and a write
	// to /status must not be able to change spec. That split is the whole point
	// of the subresource, and it is enforced by kcp -- through spillway.
	current, err := sprockets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the sprocket: %v", err)
	}
	if err := unstructured.SetNestedField(current.Object, "Running", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	updated, err := sprockets.Update(ctx, current, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating the sprocket: %v", err)
	}
	if phase, found, _ := unstructured.NestedString(updated.Object, "status", "phase"); found && phase != "" {
		t.Errorf("status.phase = %q after a write to the main resource; the subresource should have dropped it", phase)
	}

	// Now through /status, which is where a controller writes.
	if err := unstructured.SetNestedField(updated.Object, "Running", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	if err := unstructured.SetNestedField(updated.Object, int64(2), "status", "replicas"); err != nil {
		t.Fatalf("setting status.replicas: %v", err)
	}
	if err := unstructured.SetNestedField(updated.Object, int64(99), "spec", "replicas"); err != nil {
		t.Fatalf("setting spec.replicas: %v", err)
	}
	fromStatus, err := sprockets.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating the sprocket's status through the aggregation layer: %v", err)
	}
	if phase, _, _ := unstructured.NestedString(fromStatus.Object, "status", "phase"); phase != "Running" {
		t.Errorf("status.phase = %q after writing /status, want Running", phase)
	}
	if replicas, _, _ := unstructured.NestedInt64(fromStatus.Object, "spec", "replicas"); replicas != 3 {
		t.Errorf("spec.replicas = %d after writing /status, want the original 3", replicas)
	}

	// And the scale subresource, which is a different shape again: kcp answers
	// it from the paths declared on the CRD.
	scale, err := viaCluster.Resource(gvr).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{}, "scale")
	if err != nil {
		t.Fatalf("getting the scale subresource through the aggregation layer: %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(scale.Object, "spec", "replicas"); replicas != 3 {
		t.Errorf("scale spec.replicas = %d, want 3", replicas)
	}
	if scale.GetKind() != "Scale" {
		t.Errorf("scale kind = %q, want Scale", scale.GetKind())
	}
}

// TestImpersonationCarriesTheCallerToKCP turns --kcp-impersonate-users on and
// watches what changes.
//
// The flag decides whether the workspace's own RBAC applies at all, and until
// now nothing exercised it against a real kcp: the unit tests prove spillway
// sets the impersonation headers, not that kcp acts on them. The observable
// claim is that the same request, by the same client, is answered differently
// depending on the flag -- which can only be true if kcp is seeing the caller
// rather than spillway.
func TestImpersonationCarriesTheCallerToKCP(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset for the kind cluster: %v", err)
	}

	// A subject kcp has never heard of. The cluster's own RBAC lets it reach
	// spillway; whether it gets any further is the question.
	const subject = "impersonation-probe"
	if _, err := cluster.CoreV1().ServiceAccounts(namespace).Create(ctx,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: subject, Namespace: namespace}},
		metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the probe service account: %v", err)
	}
	if _, err := cluster.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: subject},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{widgetGroup},
			Resources: []string{"widgets"},
			Verbs:     []string{"get", "list", "create", "delete"},
		}},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the probe role: %v", err)
	}
	if _, err := cluster.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: subject},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: subject},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: subject, Namespace: namespace}},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("binding the probe role: %v", err)
	}
	t.Cleanup(func() {
		background := context.Background()
		_ = cluster.RbacV1().ClusterRoleBindings().Delete(background, subject, metav1.DeleteOptions{})
		_ = cluster.RbacV1().ClusterRoles().Delete(background, subject, metav1.DeleteOptions{})
		_ = cluster.CoreV1().ServiceAccounts(namespace).Delete(background, subject, metav1.DeleteOptions{})
	})

	hour := int64(3600)
	token, err := cluster.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, subject,
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &hour}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("minting a token for the probe: %v", err)
	}

	asProbe := rest.CopyConfig(kindConfig(t))
	asProbe.BearerToken = token.Status.Token
	asProbe.BearerTokenFile = ""
	asProbe.CertFile, asProbe.KeyFile = "", ""
	asProbe.CertData, asProbe.KeyData = nil, nil
	probeClient, err := dynamic.NewForConfig(asProbe)
	if err != nil {
		t.Fatalf("building the probe client: %v", err)
	}
	widgets := probeClient.Resource(widgetGVR).Namespace(namespace)

	// Without impersonation the request reaches kcp as spillway, which the
	// workspace does know.
	if _, err := widgets.List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("listing widgets as the probe with impersonation off: %v", err)
	}

	setImpersonation(ctx, t, cluster, true)
	t.Cleanup(func() { setImpersonation(context.Background(), t, cluster, false) })

	// With it on, kcp is asked about system:serviceaccount:default:impersonation-probe,
	// which has no standing in the workspace at all.
	_, err = widgets.List(ctx, metav1.ListOptions{})
	if err == nil {
		t.Fatal("listing widgets succeeded with impersonation on: kcp cannot have been " +
			"applying the workspace's RBAC to the caller")
	}
	if !apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err) {
		t.Fatalf("listing widgets with impersonation on failed with %v, want a 401 or 403 from kcp", err)
	}
	t.Logf("kcp refused the impersonated caller: %v", err)
	if strings.Contains(err.Error(), subject) {
		t.Logf("and the refusal names the impersonated subject, so kcp saw the caller's identity")
	}
}

// setImpersonation flips --kcp-impersonate-users on the running deployment and
// waits for the rollout, so the test measures the flag rather than the restart.
func setImpersonation(ctx context.Context, t *testing.T, cluster kubernetes.Interface, enabled bool) {
	t.Helper()

	const flag = "--kcp-impersonate-users"
	reconfigure(ctx, t, cluster, func(args []string) []string {
		kept := make([]string, 0, len(args)+1)
		for _, arg := range args {
			if arg != flag {
				kept = append(kept, arg)
			}
		}
		if enabled {
			kept = append(kept, flag)
		}
		return kept
	})
}

// reconfigure rewrites spillway's arguments and waits for the rollout, so a
// test measures the flag it changed rather than the restart it caused.
func reconfigure(ctx context.Context, t *testing.T, cluster kubernetes.Interface, mutate func([]string) []string) {
	t.Helper()

	deployment, err := cluster.AppsV1().Deployments("spillway-system").Get(ctx, "spillway", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the spillway deployment: %v", err)
	}

	deployment.Spec.Template.Spec.Containers[0].Args = mutate(deployment.Spec.Template.Spec.Containers[0].Args)

	if _, err := cluster.AppsV1().Deployments("spillway-system").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("reconfiguring the deployment: %v", err)
	}

	awaitRollout(ctx, t, cluster)
}

// awaitRollout waits for the deployment to be fully on its current generation,
// and then for the aggregation layer to have noticed.
//
// The second half is not belt and braces. New pods being Ready means spillway
// is answering; it does not mean kube-apiserver has re-probed the APIService,
// and until it has, a request for an offloaded resource is answered with a 503
// by the aggregator rather than reaching spillway at all. A test that starts
// issuing requests at that moment is measuring the gap.
func awaitRollout(ctx context.Context, t *testing.T, cluster kubernetes.Interface) {
	t.Helper()

	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			current, err := cluster.AppsV1().Deployments("spillway-system").Get(ctx, "spillway", metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			wanted := ptr.Deref(current.Spec.Replicas, 1)
			return current.Status.ObservedGeneration >= current.Generation &&
				current.Status.UpdatedReplicas == wanted &&
				current.Status.AvailableReplicas == wanted &&
				current.Status.Replicas == wanted, nil
		}); err != nil {
		t.Fatalf("waiting for the rollout: %v", err)
	}

	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			served, err := aggregator.ApiregistrationV1().APIServices().
				Get(ctx, "v1alpha1."+widgetGroup, metav1.GetOptions{})
			if err != nil {
				return false, nil //nolint:nilerr // it may be being re-registered
			}
			for _, condition := range served.Status.Conditions {
				if condition.Type == apiregistrationv1.Available {
					return condition.Status == apiregistrationv1.ConditionTrue, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("waiting for the aggregation layer to accept spillway again: %v", err)
	}

	// And then until it actually answers. Available is a condition the
	// availability controller writes on its own schedule, so immediately after
	// a rollout it can still be the answer from before -- true, and about pods
	// that no longer exist. The only reliable signal is a request that goes the
	// whole way.
	//
	// Discovery rather than a resource: it is served by spillway from its own
	// cache, so it says the aggregator can reach spillway without also
	// depending on what kcp makes of the caller, which some of these tests are
	// in the middle of changing.
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the discovery client: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true,
		func(context.Context) (bool, error) {
			_, err := discoveryClient.ServerResourcesForGroupVersion(widgetGroup + "/v1alpha1")
			return err == nil, nil
		}); err != nil {
		t.Fatalf("the aggregation layer never answered for spillway again: %v", err)
	}
}

// TestWildcardServesAGroupThatDidNotExist is the claim --api-group='*.domain'
// makes: a CRD in a group nobody configured, created after spillway started,
// is served through the workload cluster without touching the deployment.
//
// Everything has to line up for that to work. The CRD watch has to notice a
// group it was not told about, the discovery handler has to answer for a path
// that had no handler registered when the mux was built, the registrar has to
// create an APIService for it, and the aggregation layer has to find spillway
// serving it.
func TestWildcardServesAGroupThatDidNotExist(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset for the kind cluster: %v", err)
	}
	kcpClient, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for kcp: %v", err)
	}
	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}

	// The group is chosen to be one no flag mentions, under the same domain as
	// the configured one.
	domain := widgetGroup[strings.Index(widgetGroup, ".")+1:]
	newGroup := "unconfigured." + domain
	crdName := "cogs." + newGroup
	apiServiceName := "v1alpha1." + crdName[strings.Index(crdName, ".")+1:]

	const flagPrefix = "--api-group="
	restore := ""
	reconfigure(ctx, t, cluster, func(args []string) []string {
		rewritten := make([]string, 0, len(args))
		for _, arg := range args {
			if strings.HasPrefix(arg, flagPrefix) {
				restore = arg
				arg = flagPrefix + "*." + domain
			}
			rewritten = append(rewritten, arg)
		}
		return rewritten
	})
	if restore == "" {
		t.Fatal("the deployment carries no --api-group to replace")
	}
	t.Cleanup(func() {
		background := context.Background()
		reconfigure(background, t, cluster, func(args []string) []string {
			rewritten := make([]string, 0, len(args))
			for _, arg := range args {
				if strings.HasPrefix(arg, flagPrefix) {
					arg = restore
				}
				rewritten = append(rewritten, arg)
			}
			return rewritten
		})
		_ = aggregator.ApiregistrationV1().APIServices().Delete(background, apiServiceName, metav1.DeleteOptions{})
	})

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: newGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "cogs", Singular: "cog", Kind: "Cog", ListKind: "CogList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type:       "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{"teeth": {Type: "integer"}},
							},
						},
					},
				},
			}},
		},
	}
	if _, err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the cogs CRD in kcp: %v", err)
	}
	t.Cleanup(func() {
		_ = kcpClient.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
	})

	// Spillway was never told this group exists.
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			found, err := aggregator.ApiregistrationV1().APIServices().Get(ctx, apiServiceName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			for _, condition := range found.Status.Conditions {
				if condition.Type == apiregistrationv1.Available {
					return condition.Status == apiregistrationv1.ConditionTrue, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("waiting for %s to be registered and available: %v", apiServiceName, err)
	}

	// Registered and available is still only discovery. The objects have to
	// round trip.
	cogs := viaCluster.Resource(schema.GroupVersionResource{
		Group: newGroup, Version: "v1alpha1", Resource: "cogs"}).Namespace(namespace)

	created, err := cogs.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": newGroup + "/v1alpha1",
		"kind":       "Cog",
		"metadata":   map[string]any{"name": "one"},
		"spec":       map[string]any{"teeth": int64(12)},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a cog through the aggregation layer: %v", err)
	}

	viaKCP, err := dynamic.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client for kcp: %v", err)
	}
	stored, err := viaKCP.Resource(schema.GroupVersionResource{
		Group: newGroup, Version: "v1alpha1", Resource: "cogs"}).Namespace(namespace).
		Get(ctx, "one", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the cog created through the cluster is not in kcp: %v", err)
	}
	if stored.GetUID() != created.GetUID() {
		t.Errorf("UID in kcp = %s, through the cluster = %s", stored.GetUID(), created.GetUID())
	}

	t.Logf("%s was served, registered and stored in kcp without being configured anywhere", newGroup)
}

// TestNamespaceIsMirroredIntoTheWorkspace covers --mirror-namespaces: a
// namespace that exists only in the workload cluster is created in the
// workspace by the write that needs it, rather than by hand beforehand.
func TestNamespaceIsMirroredIntoTheWorkspace(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}
	workspace, err := kubernetes.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the clientset for kcp: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}

	const mirrored = "mirrored-e2e"
	if _, err := cluster.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: mirrored}},
		metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the namespace in the cluster: %v", err)
	}
	t.Cleanup(func() {
		background := context.Background()
		_ = cluster.CoreV1().Namespaces().Delete(background, mirrored, metav1.DeleteOptions{})
		_ = workspace.CoreV1().Namespaces().Delete(background, mirrored, metav1.DeleteOptions{})
	})

	// It must not be there yet, or the test proves nothing.
	if _, err := workspace.CoreV1().Namespaces().Get(ctx, mirrored, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the workspace already has namespace %s: %v", mirrored, err)
	}

	setFlag(ctx, t, cluster, "--mirror-namespaces", true)
	t.Cleanup(func() { setFlag(context.Background(), t, cluster, "--mirror-namespaces", false) })

	widgets := viaCluster.Resource(widgetGVR).Namespace(mirrored)
	if _, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "needs-a-namespace"},
		"spec":       map[string]any{"color": "red", "size": int64(1)},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating a widget in a namespace the workspace did not have: %v", err)
	}

	created, err := workspace.CoreV1().Namespaces().Get(ctx, mirrored, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the namespace was not mirrored into the workspace: %v", err)
	}
	if created.Labels["app.kubernetes.io/managed-by"] != "spillway" {
		t.Errorf("labels = %v, want the namespace marked as spillway's", created.Labels)
	}

	stored, err := dynamicKCP(t).Resource(widgetGVR).Namespace(mirrored).
		Get(ctx, "needs-a-namespace", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the widget is not in the workspace: %v", err)
	}
	if stored.GetName() != "needs-a-namespace" {
		t.Errorf("stored widget = %s", stored.GetName())
	}
}

// TestGroupTheClusterServesIsNotTakenOver covers the guard. An APIService takes
// precedence over the delegate behind it, so registering one for a group the
// cluster serves from its own CRDs takes that group away from it -- measured to
// leave the group's aggregated discovery entry empty in a way that does not
// repopulate. A wildcard makes that reachable by accident.
func TestGroupTheClusterServesIsNotTakenOver(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}
	local, err := apiextensionsclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for the cluster: %v", err)
	}
	inKCP, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for kcp: %v", err)
	}
	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}

	// A group under the same domain a wildcard would cover, served by the
	// cluster itself.
	domain := widgetGroup[strings.Index(widgetGroup, ".")+1:]
	contested := "contested." + domain
	crdName := "cogs." + contested

	crd := contestedCRD(crdName, contested)
	if _, err := local.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the CRD in the cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = local.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
	})
	if _, err := inKCP.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, contestedCRD(crdName, contested), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the CRD in kcp: %v", err)
	}
	t.Cleanup(func() {
		_ = inKCP.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
	})

	gvr := schema.GroupVersionResource{Group: contested, Version: "v1alpha1", Resource: "cogs"}
	cogs := viaCluster.Resource(gvr).Namespace(namespace)
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := cogs.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": contested + "/v1alpha1",
				"kind":       "Cog",
				"metadata":   map[string]any{"name": "local"},
			}}, metav1.CreateOptions{})
			return err == nil, nil
		}); err != nil {
		t.Fatalf("waiting for the cluster's own CRD to be usable: %v", err)
	}

	// Now point spillway at a wildcard that covers it.
	replaceAPIGroup(ctx, t, cluster, "*."+domain)
	t.Cleanup(func() { replaceAPIGroup(context.Background(), t, cluster, widgetGroup) })

	// Give the registrar time to have done the wrong thing, if it were going to.
	time.Sleep(30 * time.Second)

	registered, err := aggregator.ApiregistrationV1().APIServices().
		Get(ctx, "v1alpha1."+contested, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the contested APIService: %v", err)
	}
	if registered.Spec.Service != nil {
		t.Errorf("spillway pointed the cluster's own group at itself: %+v", registered.Spec.Service)
	}
	if registered.Labels["app.kubernetes.io/managed-by"] == "spillway" {
		t.Error("spillway claimed the cluster's own APIService")
	}

	// And the cluster's objects are still reachable, which is what the guard is
	// protecting.
	if _, err := cogs.Get(ctx, "local", metav1.GetOptions{}); err != nil {
		t.Errorf("the cluster's own object is no longer readable: %v", err)
	}
}

func contestedCRD(name, group string) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "cogs", Singular: "cog", Kind: "Cog", ListKind: "CogList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
				},
			}},
		},
	}
}

// TestCredentialsAreReloadedWithoutARestart covers the reload loop: the
// kubeconfig Secret is rewritten and spillway picks it up, without the restart
// that would drop every watch it is proxying.
func TestCredentialsAreReloadedWithoutARestart(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}

	before, err := cluster.CoreV1().Pods("spillway-system").List(ctx, metav1.ListOptions{LabelSelector: "app=spillway"})
	if err != nil || len(before.Items) == 0 {
		t.Fatalf("listing the spillway pods: %v", err)
	}
	started := map[string]metav1.Time{}
	for _, pod := range before.Items {
		started[pod.Name] = *pod.Status.StartTime
	}

	secret, err := cluster.CoreV1().Secrets("spillway-system").Get(ctx, "spillway-kcp-kubeconfig", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the kubeconfig secret: %v", err)
	}

	// A change that alters the file without altering what it says, so the
	// credentials keep working and only the digest moves.
	rotated := secret.DeepCopy()
	rotated.Data["kubeconfig"] = append(rotated.Data["kubeconfig"], '\n')
	if _, err := cluster.CoreV1().Secrets("spillway-system").Update(ctx, rotated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("rotating the kubeconfig secret: %v", err)
	}

	// The kubelet propagates a mounted Secret on its own sync period, and
	// spillway re-reads on its own, so this is bounded by both.
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Second, 4*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			return podsLogged(ctx, cluster, "Reloaded the kcp credentials"), nil
		}); err != nil {
		t.Fatalf("waiting for spillway to reload its credentials: %v", err)
	}

	// The point of reloading rather than restarting.
	after, err := cluster.CoreV1().Pods("spillway-system").List(ctx, metav1.ListOptions{LabelSelector: "app=spillway"})
	if err != nil {
		t.Fatalf("listing the spillway pods: %v", err)
	}
	for _, pod := range after.Items {
		if was, found := started[pod.Name]; !found || !was.Equal(pod.Status.StartTime) {
			t.Errorf("pod %s restarted; the credentials should have been reloaded in place", pod.Name)
		}
	}

	// And it is still serving with them.
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	if _, err := viaCluster.Resource(widgetGVR).Namespace(namespace).List(ctx, metav1.ListOptions{}); err != nil {
		t.Errorf("listing widgets after the reload: %v", err)
	}
}

// podsLogged reports whether any spillway pod has logged the line.
func podsLogged(ctx context.Context, cluster kubernetes.Interface, want string) bool {
	pods, err := cluster.CoreV1().Pods("spillway-system").List(ctx, metav1.ListOptions{LabelSelector: "app=spillway"})
	if err != nil {
		return false
	}
	for _, pod := range pods.Items {
		stream, err := cluster.CoreV1().Pods("spillway-system").
			GetLogs(pod.Name, &corev1.PodLogOptions{}).Stream(ctx)
		if err != nil {
			continue
		}
		logs, err := io.ReadAll(stream)
		_ = stream.Close()
		if err == nil && strings.Contains(string(logs), want) {
			return true
		}
	}
	return false
}

// setFlag adds or removes a boolean flag on the running deployment.
func setFlag(ctx context.Context, t *testing.T, cluster kubernetes.Interface, flag string, enabled bool) {
	t.Helper()

	reconfigure(ctx, t, cluster, func(args []string) []string {
		kept := make([]string, 0, len(args)+1)
		for _, arg := range args {
			if arg != flag {
				kept = append(kept, arg)
			}
		}
		if enabled {
			kept = append(kept, flag)
		}
		return kept
	})
}

// replaceAPIGroup rewrites what spillway is configured to serve.
func replaceAPIGroup(ctx context.Context, t *testing.T, cluster kubernetes.Interface, group string) {
	t.Helper()

	reconfigure(ctx, t, cluster, func(args []string) []string {
		rewritten := make([]string, 0, len(args))
		for _, arg := range args {
			if strings.HasPrefix(arg, "--api-group=") {
				arg = "--api-group=" + group
			}
			rewritten = append(rewritten, arg)
		}
		return rewritten
	})
}

// dynamicKCP is a dynamic client pointed at the workspace.
func dynamicKCP(t *testing.T) dynamic.Interface {
	t.Helper()

	client, err := dynamic.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client for kcp: %v", err)
	}
	return client
}

// TestTwoWorkspacesAreServedAtOnce is the claim --workspaces-file makes: two
// kcp workspaces behind one spillway, each backing its own API groups, with
// every object landing in the workspace its group is mapped to.
//
// Nothing else exercises more than one workspace, and the router, the merged
// discovery snapshot, the per-workspace proxies and the OpenAPI union all only
// differ from the single-workspace path when there is a second one.
func TestTwoWorkspacesAreServedAtOnce(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}

	// A second workspace beside the one the harness made, with a group of its
	// own that the first one does not serve.
	const secondGroup = "gizmos.example.net"
	secondURL := createWorkspace(ctx, t, "spillway-second")
	installGizmoCRD(ctx, t, secondURL, secondGroup)

	// Spillway reaches a workspace by the server URL in its kubeconfig, so the
	// second one gets a copy of the first's pointed elsewhere.
	first, err := os.ReadFile(os.Getenv("KCP_WORKSPACE_KUBECONFIG"))
	if err != nil {
		t.Fatalf("reading the workspace kubeconfig: %v", err)
	}
	second := rewriteServer(t, first, secondURL)

	workspacesFile := fmt.Sprintf(`workspaces:
  - name: first
    kubeconfig: /etc/spillway/workspaces/first
    apiGroups: ["%s"]
  - name: second
    kubeconfig: /etc/spillway/workspaces/second
    apiGroups: ["%s"]
`, widgetGroup, secondGroup)

	restore := useWorkspacesFile(ctx, t, cluster,
		map[string][]byte{"first": first, "second": second}, workspacesFile)
	t.Cleanup(restore)

	// Both groups have to be served, from one spillway, at once.
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	gizmoGVR := schema.GroupVersionResource{Group: secondGroup, Version: "v1alpha1", Resource: "gizmos"}

	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			found, err := aggregator.ApiregistrationV1().APIServices().
				Get(ctx, "v1alpha1."+secondGroup, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			for _, condition := range found.Status.Conditions {
				if condition.Type == apiregistrationv1.Available {
					return condition.Status == apiregistrationv1.ConditionTrue, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("waiting for the second workspace's group to be served: %v", err)
	}
	t.Cleanup(func() {
		_ = aggregator.ApiregistrationV1().APIServices().
			Delete(context.Background(), "v1alpha1."+secondGroup, metav1.DeleteOptions{})
	})

	// An object in each group, through the same endpoint.
	if _, err := viaCluster.Resource(widgetGVR).Namespace(namespace).Create(ctx, &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": widgetGroup + "/v1alpha1", "kind": "Widget",
			"metadata": map[string]any{"name": "in-the-first"},
			"spec":     map[string]any{"color": "red", "size": int64(2)},
		}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating a widget while two workspaces are configured: %v", err)
	}
	t.Cleanup(func() {
		_ = viaCluster.Resource(widgetGVR).Namespace(namespace).
			Delete(context.Background(), "in-the-first", metav1.DeleteOptions{})
	})

	if _, err := viaCluster.Resource(gizmoGVR).Namespace(namespace).Create(ctx, &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": secondGroup + "/v1alpha1", "kind": "Gizmo",
			"metadata": map[string]any{"name": "in-the-second"},
		}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating a gizmo in the second workspace: %v", err)
	}

	// And each one landed in its own workspace, not the other.
	firstClient := dynamicKCP(t)
	secondClient := dynamicFor(t, secondURL)

	if _, err := firstClient.Resource(widgetGVR).Namespace(namespace).
		Get(ctx, "in-the-first", metav1.GetOptions{}); err != nil {
		t.Errorf("the widget is not in the first workspace: %v", err)
	}
	if _, err := secondClient.Resource(gizmoGVR).Namespace(namespace).
		Get(ctx, "in-the-second", metav1.GetOptions{}); err != nil {
		t.Errorf("the gizmo is not in the second workspace: %v", err)
	}
	if _, err := firstClient.Resource(gizmoGVR).Namespace(namespace).
		Get(ctx, "in-the-second", metav1.GetOptions{}); err == nil {
		t.Error("the gizmo is in the first workspace too; the groups are not being routed")
	}

	// The configuration is re-read while spillway serves, so a workspace can be
	// taken out of it without the restart that would drop every watch being
	// proxied from the workspaces that did not change.
	pods, err := cluster.CoreV1().Pods("spillway-system").List(ctx, metav1.ListOptions{LabelSelector: "app=spillway"})
	if err != nil {
		t.Fatalf("listing the spillway pods: %v", err)
	}
	started := map[string]metav1.Time{}
	for _, pod := range pods.Items {
		started[pod.Name] = *pod.Status.StartTime
	}

	onlyTheFirst := fmt.Sprintf(`workspaces:
  - name: first
    kubeconfig: /etc/spillway/workspaces/first
    apiGroups: ["%s"]
`, widgetGroup)
	configmap, err := cluster.CoreV1().ConfigMaps("spillway-system").Get(ctx, "spillway-workspaces", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the workspaces configmap: %v", err)
	}
	configmap.Data["workspaces.yaml"] = onlyTheFirst
	if _, err := cluster.CoreV1().ConfigMaps("spillway-system").Update(ctx, configmap, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("rewriting the workspaces configmap: %v", err)
	}

	// Bounded by the kubelet propagating the ConfigMap and spillway re-reading
	// it, both on their own periods.
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 4*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := aggregator.ApiregistrationV1().APIServices().
				Get(ctx, "v1alpha1."+secondGroup, metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		}); err != nil {
		t.Fatalf("the removed workspace's APIService was not withdrawn: %v", err)
	}

	after, err := cluster.CoreV1().Pods("spillway-system").List(ctx, metav1.ListOptions{LabelSelector: "app=spillway"})
	if err != nil {
		t.Fatalf("listing the spillway pods: %v", err)
	}
	for _, pod := range after.Items {
		if was, found := started[pod.Name]; !found || !was.Equal(pod.Status.StartTime) {
			t.Errorf("pod %s restarted; the configuration should have been re-read in place", pod.Name)
		}
	}

	// The workspace that stayed is still serving.
	if _, err := viaCluster.Resource(widgetGVR).Namespace(namespace).
		Get(ctx, "in-the-first", metav1.GetOptions{}); err != nil {
		t.Errorf("the remaining workspace stopped serving: %v", err)
	}
}

// createWorkspace makes a workspace under root and returns the URL that
// addresses it.
func createWorkspace(ctx context.Context, t *testing.T, name string) string {
	t.Helper()

	root, err := dynamic.NewForConfig(rootConfig(t))
	if err != nil {
		t.Fatalf("building a client for the kcp root: %v", err)
	}
	workspaces := root.Resource(schema.GroupVersionResource{
		Group: "tenancy.kcp.io", Version: "v1alpha1", Resource: "workspaces"})

	if _, err := workspaces.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "tenancy.kcp.io/v1alpha1",
		"kind":       "Workspace",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"type": map[string]any{"name": "universal", "path": "root"}},
	}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating workspace %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = workspaces.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	var url string
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			workspace, err := workspaces.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				// Not yet visible is the usual answer here, so this polls
				// rather than failing on the first miss.
				return false, nil //nolint:nilerr // waiting for it to appear
			}
			phase, _, _ := unstructured.NestedString(workspace.Object, "status", "phase")
			url, _, _ = unstructured.NestedString(workspace.Object, "spec", "URL")
			return phase == "Ready" && url != "", nil
		}); err != nil {
		t.Fatalf("waiting for workspace %s: %v", name, err)
	}
	return url
}

func installGizmoCRD(ctx context.Context, t *testing.T, url, group string) {
	t.Helper()

	config := rootConfig(t)
	config.Host = url
	client, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		t.Fatalf("building an apiextensions client for %s: %v", url, err)
	}

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "gizmos." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "gizmos", Singular: "gizmo", Kind: "Gizmo", ListKind: "GizmoList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
				},
			}},
		},
	}
	if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil &&
		!apierrors.IsAlreadyExists(err) {
		t.Fatalf("installing the Gizmo CRD: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			established, err := client.ApiextensionsV1().CustomResourceDefinitions().
				Get(ctx, "gizmos."+group, metav1.GetOptions{})
			if err != nil {
				return false, nil //nolint:nilerr // waiting for it to appear
			}
			for _, condition := range established.Status.Conditions {
				if condition.Type == apiextensionsv1.Established {
					return condition.Status == apiextensionsv1.ConditionTrue, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("waiting for the Gizmo CRD: %v", err)
	}

	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("building a clientset for %s: %v", url, err)
	}
	if _, err := core.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}},
		metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the namespace in the second workspace: %v", err)
	}
}

// rewriteServer points a kubeconfig at another workspace.
func rewriteServer(t *testing.T, kubeconfig []byte, url string) []byte {
	t.Helper()

	config, err := clientcmd.Load(kubeconfig)
	if err != nil {
		t.Fatalf("parsing the workspace kubeconfig: %v", err)
	}
	for name := range config.Clusters {
		config.Clusters[name].Server = url
	}
	rewritten, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatalf("serialising the rewritten kubeconfig: %v", err)
	}
	return rewritten
}

func dynamicFor(t *testing.T, url string) dynamic.Interface {
	t.Helper()

	config := rootConfig(t)
	config.Host = url
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("building a client for %s: %v", url, err)
	}
	return client
}

// useWorkspacesFile swaps the deployment onto --workspaces-file and returns
// what puts it back. The kubeconfigs are mounted under
// /etc/spillway/workspaces, keyed by name, and the file itself at
// /etc/spillway/config/workspaces.yaml.
func useWorkspacesFile(ctx context.Context, t *testing.T, cluster kubernetes.Interface,
	kubeconfigs map[string][]byte, file string) func() {
	t.Helper()

	if _, err := cluster.CoreV1().Secrets("spillway-system").Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "spillway-workspaces", Namespace: "spillway-system"},
		Data:       kubeconfigs,
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the workspaces secret: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.CoreV1().Secrets("spillway-system").
			Delete(context.Background(), "spillway-workspaces", metav1.DeleteOptions{})
	})

	if _, err := cluster.CoreV1().ConfigMaps("spillway-system").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "spillway-workspaces", Namespace: "spillway-system"},
		Data:       map[string]string{"workspaces.yaml": file},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the workspaces configmap: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.CoreV1().ConfigMaps("spillway-system").
			Delete(context.Background(), "spillway-workspaces", metav1.DeleteOptions{})
	})

	deployment, err := cluster.AppsV1().Deployments("spillway-system").Get(ctx, "spillway", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the deployment: %v", err)
	}
	original := deployment.Spec.Template.DeepCopy()

	container := &deployment.Spec.Template.Spec.Containers[0]
	// The ConfigMap is mounted as a directory, not with subPath: a subPath
	// mount is never updated when the ConfigMap changes, so the file inside the
	// pod would stay as it was and the reload would have nothing to read.
	args := []string{"--workspaces-file=/etc/spillway/config/workspaces.yaml"}
	for _, arg := range container.Args {
		if strings.HasPrefix(arg, "--kcp-kubeconfig=") || strings.HasPrefix(arg, "--api-group=") {
			continue
		}
		args = append(args, arg)
	}
	container.Args = args
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{Name: "workspaces", MountPath: "/etc/spillway/workspaces", ReadOnly: true},
		corev1.VolumeMount{Name: "workspaces-file", MountPath: "/etc/spillway/config", ReadOnly: true})
	deployment.Spec.Template.Spec.Volumes = append(deployment.Spec.Template.Spec.Volumes,
		corev1.Volume{Name: "workspaces", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "spillway-workspaces"}}},
		corev1.Volume{Name: "workspaces-file", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "spillway-workspaces"}}}})

	if _, err := cluster.AppsV1().Deployments("spillway-system").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("switching the deployment onto the workspaces file: %v", err)
	}
	awaitRollout(ctx, t, cluster)

	return func() {
		background := context.Background()
		current, err := cluster.AppsV1().Deployments("spillway-system").Get(background, "spillway", metav1.GetOptions{})
		if err != nil {
			t.Errorf("restoring the deployment: %v", err)
			return
		}
		current.Spec.Template = *original
		if _, err := cluster.AppsV1().Deployments("spillway-system").Update(background, current, metav1.UpdateOptions{}); err != nil {
			t.Errorf("restoring the deployment: %v", err)
			return
		}
		awaitRollout(background, t, cluster)
	}
}

// TestServerSideApplyThroughAggregationLayer covers apply, which is a PATCH
// with a content type of its own and semantics spillway cannot resolve itself:
// field ownership lives in kcp. Spillway asks kcp to resolve it with a dry run
// when a webhook needs to see the result, and forwards it untouched otherwise --
// both paths have to keep apply behaving as apply.
func TestServerSideApplyThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	widgets := viaCluster.Resource(widgetGVR).Namespace(namespace)

	const name = "applied"
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	desired := func(color string, size int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": widgetGroup + "/v1alpha1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"color": color, "size": size},
		}}
	}

	applied, err := widgets.Apply(ctx, name, desired("red", 3), metav1.ApplyOptions{FieldManager: "first"})
	if err != nil {
		t.Fatalf("applying through the aggregation layer: %v", err)
	}
	if size, _, _ := unstructured.NestedInt64(applied.Object, "spec", "size"); size != 3 {
		t.Errorf("spec.size = %d after apply, want 3", size)
	}

	// Ownership has to have been recorded, or the conflict below would be
	// meaningless: apply is only apply if kcp is tracking who owns what.
	managers := map[string]bool{}
	for _, entry := range applied.GetManagedFields() {
		managers[entry.Manager] = true
	}
	if !managers["first"] {
		t.Errorf("managedFields = %+v, want an entry for the applying manager", applied.GetManagedFields())
	}

	// A second manager taking a field the first owns must conflict.
	_, err = widgets.Apply(ctx, name, desired("blue", 3), metav1.ApplyOptions{FieldManager: "second"})
	if err == nil {
		t.Error("a second manager overwrote a field owned by the first without a conflict")
	} else if !apierrors.IsConflict(err) {
		t.Errorf("second apply failed with %v, want a conflict", err)
	}

	// And force resolves it, transferring ownership.
	forced, err := widgets.Apply(ctx, name, desired("blue", 3),
		metav1.ApplyOptions{FieldManager: "second", Force: true})
	if err != nil {
		t.Fatalf("forcing the apply: %v", err)
	}
	if color, _, _ := unstructured.NestedString(forced.Object, "spec", "color"); color != "blue" {
		t.Errorf("spec.color = %q after a forced apply, want blue", color)
	}

	// The same object, as kcp holds it.
	stored, err := dynamicKCP(t).Resource(widgetGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the applied object from kcp: %v", err)
	}
	if color, _, _ := unstructured.NestedString(stored.Object, "spec", "color"); color != "blue" {
		t.Errorf("spec.color in kcp = %q, want blue", color)
	}
}

// TestPaginationThroughAggregationLayer covers limit and continue. The continue
// token is kcp's, opaque to spillway, and has to survive the round trip through
// it -- a proxy that rewrote or dropped a query parameter would silently turn
// pagination into "the first page, forever".
func TestPaginationThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	widgets := viaCluster.Resource(widgetGVR).Namespace(namespace)

	const count = 12
	for i := range count {
		name := fmt.Sprintf("paged-%02d", i)
		if _, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": widgetGroup + "/v1alpha1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"color": "red", "size": int64(1)},
		}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
		t.Cleanup(func() {
			_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
		})
	}

	seen := map[string]int{}
	pages, token := 0, ""
	for {
		page, err := widgets.List(ctx, metav1.ListOptions{Limit: 5, Continue: token})
		if err != nil {
			t.Fatalf("listing page %d: %v", pages+1, err)
		}
		pages++

		if len(page.Items) > 5 {
			t.Fatalf("page %d has %d items, want at most the limit of 5", pages, len(page.Items))
		}
		for _, item := range page.Items {
			seen[item.GetName()]++
		}

		token = page.GetContinue()
		if token == "" {
			break
		}
		if pages > 10 {
			t.Fatal("pagination did not terminate; the continue token is not advancing")
		}
	}

	if pages < 2 {
		t.Errorf("the whole list came back in %d page(s); the limit was not applied", pages)
	}
	for i := range count {
		name := fmt.Sprintf("paged-%02d", i)
		if seen[name] != 1 {
			t.Errorf("%s appeared %d times across the pages, want exactly once", name, seen[name])
		}
	}
}

// TestFieldSelectorThroughAggregationLayer covers a selector being applied by
// kcp rather than being dropped on the way. A dropped selector returns more
// than was asked for, which a client that trusts it will treat as a match.
func TestFieldSelectorThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	widgets := viaCluster.Resource(widgetGVR).Namespace(namespace)

	all, err := widgets.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing widgets: %v", err)
	}
	if len(all.Items) < 2 {
		t.Fatalf("only %d widgets; the selector would prove nothing", len(all.Items))
	}
	wanted := all.Items[0].GetName()

	selected, err := widgets.List(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + wanted})
	if err != nil {
		t.Fatalf("listing with a field selector: %v", err)
	}
	if len(selected.Items) != 1 || selected.Items[0].GetName() != wanted {
		names := make([]string, 0, len(selected.Items))
		for _, item := range selected.Items {
			names = append(names, item.GetName())
		}
		t.Errorf("selector metadata.name=%s returned %v, want just it", wanted, names)
	}

	// One that matches nothing must return nothing, rather than everything.
	empty, err := widgets.List(ctx, metav1.ListOptions{FieldSelector: "metadata.name=no-such-widget"})
	if err != nil {
		t.Fatalf("listing with a selector that matches nothing: %v", err)
	}
	if len(empty.Items) != 0 {
		t.Errorf("a selector matching nothing returned %d items", len(empty.Items))
	}

	// And a label selector, which is the one controllers actually use.
	labelled, err := widgets.List(ctx, metav1.ListOptions{LabelSelector: "no-such-label=value"})
	if err != nil {
		t.Fatalf("listing with a label selector: %v", err)
	}
	if len(labelled.Items) != 0 {
		t.Errorf("a label selector matching nothing returned %d items", len(labelled.Items))
	}
}

// TestMultipleVersionsThroughAggregationLayer covers a CRD served at two
// versions: both have to appear in discovery, both have to be reachable, and an
// object written at one has to be readable at the other, which is kcp
// converting between them underneath spillway.
func TestMultipleVersionsThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	kcpClient, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for kcp: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}

	const crdName = "levers." + widgetGroup
	schemaFor := func() *apiextensionsv1.CustomResourceValidation {
		return &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"spec": {
						Type:       "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{"position": {Type: "string"}},
					},
				},
			},
		}
	}

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: widgetGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "levers", Singular: "lever", Kind: "Lever", ListKind: "LeverList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			// No conversion webhook: the versions share a schema, so kcp serves
			// both from one stored form. A webhook would be kcp's to dial, not
			// spillway's, and needs a TLS server inside the workspace.
			Conversion: &apiextensionsv1.CustomResourceConversion{Strategy: apiextensionsv1.NoneConverter},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1alpha1", Served: true, Storage: true, Schema: schemaFor()},
				{Name: "v1beta1", Served: true, Storage: false, Schema: schemaFor()},
			},
		},
	}
	if _, err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the two-version CRD in kcp: %v", err)
	}
	t.Cleanup(func() {
		_ = kcpClient.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
		_ = aggregator.ApiregistrationV1().APIServices().
			Delete(context.Background(), "v1beta1."+widgetGroup, metav1.DeleteOptions{})
	})

	alpha := schema.GroupVersionResource{Group: widgetGroup, Version: "v1alpha1", Resource: "levers"}
	beta := schema.GroupVersionResource{Group: widgetGroup, Version: "v1beta1", Resource: "levers"}

	const name = "throttle"
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := viaCluster.Resource(alpha).Namespace(namespace).Create(ctx, &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": widgetGroup + "/v1alpha1", "kind": "Lever",
					"metadata": map[string]any{"name": name},
					"spec":     map[string]any{"position": "open"},
				}}, metav1.CreateOptions{})
			return err == nil, nil
		}); err != nil {
		t.Fatalf("waiting for levers to be servable at v1alpha1: %v", err)
	}
	t.Cleanup(func() {
		_ = viaCluster.Resource(alpha).Namespace(namespace).Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	// The second version has to be reachable too, which needs spillway to have
	// registered an APIService for it and to serve its discovery.
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := viaCluster.Resource(beta).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			return err == nil, nil //nolint:nilerr // waiting for the version to be served
		}); err != nil {
		t.Fatalf("the object written at v1alpha1 is not readable at v1beta1: %v", err)
	}

	atBeta, err := viaCluster.Resource(beta).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading at v1beta1: %v", err)
	}
	if atBeta.GetAPIVersion() != widgetGroup+"/v1beta1" {
		t.Errorf("apiVersion = %q, want the version it was asked for", atBeta.GetAPIVersion())
	}
	if position, _, _ := unstructured.NestedString(atBeta.Object, "spec", "position"); position != "open" {
		t.Errorf("spec.position at v1beta1 = %q, want open", position)
	}

	// Both versions have to be in the discovery spillway publishes.
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the discovery client: %v", err)
	}
	for _, version := range []string{"v1alpha1", "v1beta1"} {
		resources, err := discoveryClient.ServerResourcesForGroupVersion(widgetGroup + "/" + version)
		if err != nil {
			t.Fatalf("discovery for %s: %v", version, err)
		}
		found := false
		for _, resource := range resources.APIResources {
			if resource.Name == "levers" {
				found = true
			}
		}
		if !found {
			t.Errorf("levers is missing from discovery at %s", version)
		}
	}
}

// TestWebhookIsCalledForServerSideApply is the combination the other tests miss:
// a server-side apply while a webhook wants to see writes to the resource.
//
// Spillway admits a patch by asking kcp to resolve it with a dry run. An apply
// is a patch, but one the API server refuses without a fieldManager -- so if
// anything is lost between the caller's request and that dry run, the
// resolution fails, and a failed resolution means the write is forwarded
// unadmitted. The webhook here is unreachable with a failure policy of Fail, so
// a write that reaches it is refused: succeeding means admission was skipped.
func TestWebhookIsCalledForServerSideApply(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	widgets := viaCluster.Resource(widgetGVR).Namespace(namespace)

	const configName = "spillway-e2e-apply-webhook"
	unreachable := "https://no-such-webhook.invalid/validate"
	sideEffects := admissionregistrationv1.SideEffectClassNone
	fail := admissionregistrationv1.Fail

	if _, err := cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx,
		&admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: configName},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{
				Name:                    "widgets.apply.spillway.e2e",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: &unreachable},
				FailurePolicy:           &fail,
				SideEffects:             &sideEffects,
				AdmissionReviewVersions: []string{"v1"},
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{
						admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{widgetGroup},
						APIVersions: []string{"v1alpha1"},
						Resources:   []string{"widgets"},
					},
				}},
			}},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("registering the webhook: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().
			Delete(context.Background(), configName, metav1.DeleteOptions{})
	})

	const name = "applied-under-a-webhook"
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	desired := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"color": "red", "size": int64(4)},
	}}

	// Spillway reads the configurations through an informer, so give it a
	// moment, and let a plain create prove the webhook is being consulted at all
	// before drawing any conclusion from the apply.
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": widgetGroup + "/v1alpha1",
				"kind":       "Widget",
				"metadata":   map[string]any{"name": "control-for-apply"},
				"spec":       map[string]any{"color": "red"},
			}}, metav1.CreateOptions{})
			if err == nil {
				_ = widgets.Delete(ctx, "control-for-apply", metav1.DeleteOptions{})
				return false, nil
			}
			return strings.Contains(err.Error(), "no-such-webhook.invalid"), nil
		}); err != nil {
		t.Fatalf("the webhook is not being consulted for creates, so this proves nothing: %v", err)
	}

	_, err = widgets.Apply(ctx, name, desired, metav1.ApplyOptions{FieldManager: "e2e"})
	if err == nil {
		t.Fatal("a server-side apply succeeded while a webhook with failurePolicy=Fail wanted to see " +
			"writes to this resource: admission was skipped")
	}
	if !strings.Contains(err.Error(), "no-such-webhook.invalid") {
		t.Errorf("the apply was refused by %v, want the webhook", err)
	}
	t.Logf("the webhook was consulted for a server-side apply: %v", err)
}

// TestBoundAPIExportIsServed covers kcp's own mechanism for sharing APIs
// between workspaces. A resource that arrives in a workspace through an
// APIBinding is not backed by a CustomResourceDefinition there at all -- it is
// a schema exported by another workspace -- so it is worth knowing whether
// spillway serves it like anything else, or whether the CRD watch and the
// OpenAPI v2 synthesis quietly assume a CRD.
func TestBoundAPIExportIsServed(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}

	const (
		boundGroup = "export.example.com"
		exportName = "gauges"
		schemaName = "v1alpha1.gauges." + boundGroup
	)

	// Spillway is put on the wildcard first, so that when the binding is made
	// nothing else changes: no restart, no reconfiguration. What is left to
	// notice the new group is the watch.
	domain := widgetGroup[strings.Index(widgetGroup, ".")+1:]
	replaceAPIGroup(ctx, t, cluster, "*."+domain)
	t.Cleanup(func() { replaceAPIGroup(context.Background(), t, cluster, widgetGroup) })

	// A provider workspace, exporting a schema of its own.
	providerURL := createWorkspace(ctx, t, "spillway-provider")
	provider := dynamicFor(t, providerURL)

	schemaGVR := schema.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha1", Resource: "apiresourceschemas"}
	if _, err := provider.Resource(schemaGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apis.kcp.io/v1alpha1",
		"kind":       "APIResourceSchema",
		"metadata":   map[string]any{"name": schemaName},
		"spec": map[string]any{
			"group": boundGroup,
			"scope": "Namespaced",
			"names": map[string]any{
				"plural": "gauges", "singular": "gauge", "kind": "Gauge", "listKind": "GaugeList",
			},
			"versions": []any{map[string]any{
				"name": "v1alpha1", "served": true, "storage": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"spec": map[string]any{
							"type":       "object",
							"properties": map[string]any{"reading": map[string]any{"type": "integer"}},
						},
					},
				},
			}},
		},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the APIResourceSchema: %v", err)
	}

	exportGVR := schema.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha2", Resource: "apiexports"}
	if _, err := provider.Resource(exportGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apis.kcp.io/v1alpha2",
		"kind":       "APIExport",
		"metadata":   map[string]any{"name": exportName},
		"spec": map[string]any{
			"resources": []any{map[string]any{
				"group": boundGroup, "name": "gauges", "schema": schemaName,
				"storage": map[string]any{"crd": map[string]any{}},
			}},
		},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the APIExport: %v", err)
	}

	// The workspace spillway serves binds to it.
	consumer := dynamicKCP(t)
	bindingGVR := schema.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha2", Resource: "apibindings"}
	if _, err := consumer.Resource(bindingGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apis.kcp.io/v1alpha2",
		"kind":       "APIBinding",
		"metadata":   map[string]any{"name": "gauges"},
		"spec": map[string]any{
			"reference": map[string]any{
				"export": map[string]any{"path": "root:spillway-provider", "name": exportName},
			},
		},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the APIBinding: %v", err)
	}
	t.Cleanup(func() {
		_ = consumer.Resource(bindingGVR).Delete(context.Background(), "gauges", metav1.DeleteOptions{})
	})

	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			binding, err := consumer.Resource(bindingGVR).Get(ctx, "gauges", metav1.GetOptions{})
			if err != nil {
				return false, nil //nolint:nilerr // waiting for it to settle
			}
			phase, _, _ := unstructured.NestedString(binding.Object, "status", "phase")
			return phase == "Bound", nil
		}); err != nil {
		binding, _ := consumer.Resource(bindingGVR).Get(ctx, "gauges", metav1.GetOptions{})
		t.Fatalf("the APIBinding never became Bound: %v (status: %v)", err,
			binding.Object["status"])
	}

	// The workspace now serves a group nobody gave it a CRD for.
	gaugeGVR := schema.GroupVersionResource{Group: boundGroup, Version: "v1alpha1", Resource: "gauges"}
	if _, err := consumer.Resource(gaugeGVR).Namespace(namespace).List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("the bound resource is not servable in the workspace itself: %v", err)
	}

	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}
	t.Cleanup(func() {
		_ = aggregator.ApiregistrationV1().APIServices().
			Delete(context.Background(), "v1alpha1."+boundGroup, metav1.DeleteOptions{})
	})

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	gauges := viaCluster.Resource(gaugeGVR).Namespace(namespace)

	// Two minutes is well inside the backstop refresh, which is ten by default:
	// passing here means something noticed the binding rather than the clock
	// eventually coming round.
	bound := time.Now()
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := gauges.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": boundGroup + "/v1alpha1",
				"kind":       "Gauge",
				"metadata":   map[string]any{"name": "bound"},
				"spec":       map[string]any{"reading": int64(11)},
			}}, metav1.CreateOptions{})
			return err == nil, nil
		}); err != nil {
		t.Fatalf("a resource bound from an APIExport is not servable through spillway: %v", err)
	}
	t.Logf("the bound API reached the workload cluster in %s", time.Since(bound).Round(time.Second))
	t.Cleanup(func() {
		_ = gauges.Delete(context.Background(), "bound", metav1.DeleteOptions{})
	})

	// And it is stored in the workspace, not somewhere spillway invented.
	stored, err := consumer.Resource(gaugeGVR).Namespace(namespace).Get(ctx, "bound", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the object written through spillway is not in the workspace: %v", err)
	}
	if reading, _, _ := unstructured.NestedInt64(stored.Object, "spec", "reading"); reading != 11 {
		t.Errorf("spec.reading = %d in the workspace, want 11", reading)
	}

	t.Logf("a resource bound from an APIExport in %s is served through the workload cluster", providerURL)
}

// TestVirtualWorkspaceIsServed points spillway at an APIExport's virtual
// endpoint instead of at a workspace.
//
// kcp serves one of these per export, at a URL that aggregates the objects of
// every workspace bound to it. It is a plain Kubernetes API surface at a URL
// with a path, which is exactly what spillway addresses a workspace by, so it
// ought to work -- and "ought to" is why this exists. What it proves is that a
// provider can offer one endpoint carrying every consumer's objects, and that a
// cluster can be given that through spillway without knowing any of it.
func TestVirtualWorkspaceIsServed(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}

	const (
		exportedGroup = "dials.example.net"
		exportName    = "dials"
		schemaName    = "v1alpha1.dials." + exportedGroup
	)

	providerURL := createWorkspace(ctx, t, "spillway-vw-provider")
	provider := dynamicFor(t, providerURL)

	schemaGVR := schema.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha1", Resource: "apiresourceschemas"}
	if _, err := provider.Resource(schemaGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apis.kcp.io/v1alpha1",
		"kind":       "APIResourceSchema",
		"metadata":   map[string]any{"name": schemaName},
		"spec": map[string]any{
			"group": exportedGroup,
			"scope": "Namespaced",
			"names": map[string]any{
				"plural": "dials", "singular": "dial", "kind": "Dial", "listKind": "DialList",
			},
			"versions": []any{map[string]any{
				"name": "v1alpha1", "served": true, "storage": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"spec": map[string]any{
							"type":       "object",
							"properties": map[string]any{"setting": map[string]any{"type": "string"}},
						},
					},
				},
			}},
		},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the APIResourceSchema: %v", err)
	}

	exportGVR := schema.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha2", Resource: "apiexports"}
	if _, err := provider.Resource(exportGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apis.kcp.io/v1alpha2",
		"kind":       "APIExport",
		"metadata":   map[string]any{"name": exportName},
		"spec": map[string]any{
			"resources": []any{map[string]any{
				"group": exportedGroup, "name": "dials", "schema": schemaName,
				"storage": map[string]any{"crd": map[string]any{}},
			}},
		},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the APIExport: %v", err)
	}

	// The workspace the harness made binds to it, and gets an object of its
	// own. That object is what has to appear through the virtual endpoint.
	consumer := dynamicKCP(t)
	bindingGVR := schema.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha2", Resource: "apibindings"}
	if _, err := consumer.Resource(bindingGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apis.kcp.io/v1alpha2",
		"kind":       "APIBinding",
		"metadata":   map[string]any{"name": "dials"},
		"spec": map[string]any{
			"reference": map[string]any{
				"export": map[string]any{"path": "root:spillway-vw-provider", "name": exportName},
			},
		},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the APIBinding: %v", err)
	}
	t.Cleanup(func() {
		_ = consumer.Resource(bindingGVR).Delete(context.Background(), "dials", metav1.DeleteOptions{})
	})

	dialGVR := schema.GroupVersionResource{Group: exportedGroup, Version: "v1alpha1", Resource: "dials"}
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := consumer.Resource(dialGVR).Namespace(namespace).Create(ctx, &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": exportedGroup + "/v1alpha1", "kind": "Dial",
					"metadata": map[string]any{"name": "in-the-consumer"},
					"spec":     map[string]any{"setting": "high"},
				}}, metav1.CreateOptions{})
			return err == nil, nil
		}); err != nil {
		t.Fatalf("waiting for the binding to make dials writable in the consumer workspace: %v", err)
	}
	t.Cleanup(func() {
		_ = consumer.Resource(dialGVR).Namespace(namespace).
			Delete(context.Background(), "in-the-consumer", metav1.DeleteOptions{})
	})

	// The virtual endpoint's URL: the shard's own, the provider's logical
	// cluster, the export, and the wildcard that spans every binding.
	virtual := virtualWorkspaceURL(ctx, t, "spillway-vw-provider", exportName)
	t.Logf("virtual workspace: %s", virtual)

	// Spillway is given it exactly as it is given a workspace.
	base, err := os.ReadFile(os.Getenv("KCP_WORKSPACE_KUBECONFIG"))
	if err != nil {
		t.Fatalf("reading the workspace kubeconfig: %v", err)
	}
	file := fmt.Sprintf(`workspaces:
  - name: workspace
    kubeconfig: /etc/spillway/workspaces/workspace
    apiGroups: ["%s"]
  - name: virtual
    kubeconfig: /etc/spillway/workspaces/virtual
    apiGroups: ["%s"]
`, widgetGroup, exportedGroup)

	restore := useWorkspacesFile(ctx, t, cluster, map[string][]byte{
		"workspace": base,
		"virtual":   rewriteServer(t, base, virtual),
	}, file)
	t.Cleanup(restore)

	aggregator, err := aggregatorclient.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the aggregator client: %v", err)
	}
	t.Cleanup(func() {
		_ = aggregator.ApiregistrationV1().APIServices().
			Delete(context.Background(), "v1alpha1."+exportedGroup, metav1.DeleteOptions{})
	})

	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}
	dials := viaCluster.Resource(dialGVR)

	// The object was created in the consumer workspace, and spillway has never
	// been pointed at that workspace for this group. If it turns up here it can
	// only have come through the export's virtual endpoint.
	//
	// Across namespaces, because that is what a wildcard endpoint serves: it
	// spans workspaces, so a request scoped to one namespace of one of them is
	// not a question it can answer.
	var found *unstructured.UnstructuredList
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			list, err := dials.List(ctx, metav1.ListOptions{})
			if err != nil || len(list.Items) == 0 {
				return false, nil //nolint:nilerr // waiting for the group to be served
			}
			found = list
			return true, nil
		}); err != nil {
		t.Fatalf("the exported resource is not servable through the virtual endpoint: %v", err)
	}

	if len(found.Items) != 1 || found.Items[0].GetName() != "in-the-consumer" {
		t.Fatalf("listed %d items, want the consumer's object", len(found.Items))
	}
	if setting, _, _ := unstructured.NestedString(found.Items[0].Object, "spec", "setting"); setting != "high" {
		t.Errorf("spec.setting = %q, want the consumer's object", setting)
	}
	// The object carries which workspace it came from, which is the whole point
	// of the endpoint spanning them.
	if cluster := found.Items[0].GetAnnotations()["kcp.io/cluster"]; cluster == "" {
		t.Error("the object does not say which workspace it came from")
	}

	// What a wildcard endpoint does not serve, so that the limitation is
	// recorded as kcp's rather than rediscovered as a bug in spillway. Both of
	// these errors come from kcp, through spillway, unchanged.
	if _, err := viaCluster.Resource(dialGVR).Namespace(namespace).
		List(ctx, metav1.ListOptions{}); err == nil {
		t.Error("a namespaced list succeeded against a cross-workspace endpoint")
	} else {
		t.Logf("namespaced list is refused, as it must be: %v", err)
	}
	if _, err := viaCluster.Resource(dialGVR).Namespace(namespace).
		Get(ctx, "in-the-consumer", metav1.GetOptions{}); err == nil {
		t.Error("a get by name succeeded against a cross-workspace endpoint")
	}

	t.Log("a consumer workspace's object reached the cluster through the export's virtual endpoint")
}

// virtualWorkspaceURL builds the URL kcp serves an APIExport's virtual
// workspace at: the shard's own base, the provider's logical cluster, the
// export, and the wildcard that spans every workspace bound to it.
func virtualWorkspaceURL(ctx context.Context, t *testing.T, workspace, export string) string {
	t.Helper()

	root, err := dynamic.NewForConfig(rootConfig(t))
	if err != nil {
		t.Fatalf("building a client for the kcp root: %v", err)
	}

	shards, err := root.Resource(schema.GroupVersionResource{
		Group: "core.kcp.io", Version: "v1alpha1", Resource: "shards"}).List(ctx, metav1.ListOptions{})
	if err != nil || len(shards.Items) == 0 {
		t.Fatalf("listing kcp shards: %v", err)
	}
	base, _, _ := unstructured.NestedString(shards.Items[0].Object, "spec", "virtualWorkspaceURL")
	if base == "" {
		t.Fatal("the shard advertises no virtual workspace URL")
	}

	found, err := root.Resource(schema.GroupVersionResource{
		Group: "tenancy.kcp.io", Version: "v1alpha1", Resource: "workspaces"}).
		Get(ctx, workspace, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting workspace %s: %v", workspace, err)
	}
	logical, _, _ := unstructured.NestedString(found.Object, "spec", "cluster")
	if logical == "" {
		t.Fatalf("workspace %s has no logical cluster", workspace)
	}

	return fmt.Sprintf("%s/services/apiexport/%s/%s/clusters/*", strings.TrimSuffix(base, "/"), logical, export)
}

// TestClusterScopedResourceThroughAggregationLayer covers a resource with no
// namespace at all.
//
// Every other resource in this suite is namespaced, and a cluster-scoped one
// takes a different path through all of it: the request has no
// /namespaces/<name>/ segment, admission builds its attributes with an empty
// namespace, and the namespace mirror has nothing to mirror. Each of those is
// somewhere spillway could assume a namespace exists.
func TestClusterScopedResourceThroughAggregationLayer(t *testing.T) {
	ctx := testContext(t)

	cluster, err := kubernetes.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}
	kcpClient, err := apiextensionsclient.NewForConfig(kcpConfig(t))
	if err != nil {
		t.Fatalf("building the apiextensions client for kcp: %v", err)
	}
	viaCluster, err := dynamic.NewForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the dynamic client: %v", err)
	}

	const crdName = "beacons." + widgetGroup
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: widgetGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "beacons", Singular: "beacon", Kind: "Beacon", ListKind: "BeaconList",
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type:       "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{"lit": {Type: "boolean"}},
							},
						},
					},
				},
			}},
		},
	}
	if _, err := kcpClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the cluster-scoped CRD in kcp: %v", err)
	}
	t.Cleanup(func() {
		_ = kcpClient.ApiextensionsV1().CustomResourceDefinitions().
			Delete(context.Background(), crdName, metav1.DeleteOptions{})
	})

	gvr := schema.GroupVersionResource{Group: widgetGroup, Version: "v1alpha1", Resource: "beacons"}
	// No .Namespace(): that is the whole point.
	beacons := viaCluster.Resource(gvr)

	// Discovery first, and waited for: spillway proxies a resource request by
	// path without consulting its own resource list, so a create would succeed
	// the moment kcp established the CRD and prove nothing about whether the
	// cluster can see the resource at all.
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kindConfig(t))
	if err != nil {
		t.Fatalf("building the discovery client: %v", err)
	}

	var described *metav1.APIResource
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(context.Context) (bool, error) {
			resources, err := discoveryClient.ServerResourcesForGroupVersion(widgetGroup + "/v1alpha1")
			if err != nil {
				return false, nil //nolint:nilerr // waiting for it to appear
			}
			for i := range resources.APIResources {
				if resources.APIResources[i].Name == "beacons" {
					described = &resources.APIResources[i]
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("a cluster-scoped resource never reached the cluster's discovery: %v", err)
	}

	// It has to say it is not namespaced, or clients will address it with a
	// namespace and get nothing.
	if described.Namespaced {
		t.Error("discovery reports the cluster-scoped resource as namespaced")
	}

	const name = "north"
	if _, err := beacons.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetGroup + "/v1alpha1",
		"kind":       "Beacon",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"lit": true},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating a cluster-scoped object: %v", err)
	}
	t.Cleanup(func() {
		_ = beacons.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	// It is in kcp, and it is the same object.
	stored, err := dynamicKCP(t).Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the object is not in kcp: %v", err)
	}
	if lit, _, _ := unstructured.NestedBool(stored.Object, "spec", "lit"); !lit {
		t.Error("spec.lit did not reach kcp")
	}
	if stored.GetNamespace() != "" {
		t.Errorf("kcp stored it in namespace %q", stored.GetNamespace())
	}

	// The rest of the verbs, on the path with no namespace in it.
	fetched, err := beacons.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting: %v", err)
	}
	if err := unstructured.SetNestedField(fetched.Object, false, "spec", "lit"); err != nil {
		t.Fatalf("setting spec.lit: %v", err)
	}
	if _, err := beacons.Update(ctx, fetched, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating: %v", err)
	}
	if _, err := beacons.Patch(ctx, name, types.MergePatchType,
		[]byte(`{"spec":{"lit":true}}`), metav1.PatchOptions{}); err != nil {
		t.Fatalf("patching: %v", err)
	}

	listed, err := beacons.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(listed.Items) != 1 || listed.Items[0].GetName() != name {
		t.Errorf("listed %d items, want the one", len(listed.Items))
	}

	if err := beacons.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting: %v", err)
	}
	if _, err := dynamicKCP(t).Resource(gvr).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("after deleting through the cluster, kcp still has it: %v", err)
	}

	// And admission runs for it. This is the path where an empty namespace could
	// have gone wrong: the attributes are built without one, and a webhook that
	// wants to see writes to this resource has to be consulted anyway.
	const configName = "spillway-e2e-cluster-scoped"
	unreachable := "https://no-such-webhook.invalid/validate"
	sideEffects := admissionregistrationv1.SideEffectClassNone
	fail := admissionregistrationv1.Fail

	if _, err := cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx,
		&admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: configName},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{
				Name:                    "beacons.spillway.e2e",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: &unreachable},
				FailurePolicy:           &fail,
				SideEffects:             &sideEffects,
				AdmissionReviewVersions: []string{"v1"},
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{widgetGroup},
						APIVersions: []string{"v1alpha1"},
						Resources:   []string{"beacons"},
					},
				}},
			}},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("registering the webhook: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().
			Delete(context.Background(), configName, metav1.DeleteOptions{})
	})

	if err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			_, err := beacons.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": widgetGroup + "/v1alpha1",
				"kind":       "Beacon",
				"metadata":   map[string]any{"name": "south"},
			}}, metav1.CreateOptions{})
			if err == nil {
				_ = beacons.Delete(ctx, "south", metav1.DeleteOptions{})
				return false, nil
			}
			return strings.Contains(err.Error(), "no-such-webhook.invalid"), nil
		}); err != nil {
		t.Errorf("a webhook wanting cluster-scoped creates was not consulted: %v", err)
	} else {
		t.Log("admission ran for a cluster-scoped create")
	}
}
