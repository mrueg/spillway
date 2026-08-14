//go:build bench

// Package bench compares serving a custom resource from the cluster's own
// apiserver against serving the same resource from kcp through spillway.
//
// The comparison is deliberately unflattering to spillway on latency. A native
// custom resource is one hop: client, kube-apiserver, etcd. An offloaded one is
// three: client, kube-apiserver acting as aggregator, spillway, kcp, kcp's
// etcd. Spillway cannot be faster, and a benchmark that suggested otherwise
// would be measuring something wrong.
//
// What it is for is the other column: what the cluster's own apiserver and etcd
// stop carrying when the resource moves.
package bench

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kindKubeconfig = os.Getenv("KIND_KUBECONFIG")
	namespace      = envOrDefault("E2E_NAMESPACE", "default")

	// offloadedGroup is served by spillway out of kcp; nativeGroup is an
	// ordinary CRD this benchmark installs in the cluster itself.
	offloadedGroup = envOrDefault("API_GROUP", "spillway.example.com")
	nativeGroup    = envOrDefault("BENCH_NATIVE_GROUP", "native.example.com")

	objects = envInt("BENCH_OBJECTS", 3000)
	runs    = envInt("BENCH_RUNS", 5)

	// The measured traffic runs as an ordinary RBAC subject by default. An
	// admin is in system:masters, which the delegating authorizer
	// short-circuits, so measuring as one skips a SubjectAccessReview per
	// request that real clients pay.
	asNormalUser = envOrDefault("BENCH_NORMAL_USER", "true") != "false"
	warmup       = envInt("BENCH_WARMUP", 100)
	concurrency  = envInt("BENCH_CONCURRENCY", 16)
	reads        = envInt("BENCH_READS", 400)
	lists        = envInt("BENCH_LISTS", 30)
	watches      = envInt("BENCH_WATCH_EVENTS", 50)
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

func config(t *testing.T) *rest.Config {
	t.Helper()

	if kindKubeconfig == "" {
		t.Fatal("KIND_KUBECONFIG is unset -- run 'hack/e2e.sh up' first")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kindKubeconfig)
	if err != nil {
		t.Fatalf("loading the kubeconfig: %v", err)
	}
	// The client's own throttle would otherwise be what is measured.
	cfg.QPS = 2000
	cfg.Burst = 4000
	return cfg
}

// phaseResult holds every run's outcome for one phase, per backend.
type phaseResult struct {
	phase  string
	result map[string]*series
}

// series is one phase measured repeatedly. A single run of this benchmark is
// not worth much: the same code path produced p50s of 210ms and 109ms in
// consecutive runs while this was being written, so what is reported is the
// median across runs and the spread around it.
type series struct {
	runs []*sample
}

func (s *series) add(sample *sample) { s.runs = append(s.runs, sample) }

func (s *series) medianThroughput() float64 {
	values := make([]float64, 0, len(s.runs))
	for _, run := range s.runs {
		values = append(values, run.throughput())
	}
	sort.Float64s(values)
	if len(values) == 0 {
		return 0
	}
	return values[len(values)/2]
}

func (s *series) percentileAcrossRuns(p float64) (median, low, high time.Duration) {
	values := make([]time.Duration, 0, len(s.runs))
	for _, run := range s.runs {
		values = append(values, run.percentile(p))
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return 0, 0, 0
	}
	return values[len(values)/2], values[0], values[len(values)-1]
}

func (s *series) failures() int {
	var total int
	for _, run := range s.runs {
		total += run.failures
	}
	return total
}

// backend is one of the two things being compared. Both are reached through the
// same cluster endpoint with the same client, so the only difference is where
// the objects live.
type backend struct {
	name   string
	client dynamic.NamespaceableResourceInterface
}

func widget(group, name string, size int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": group + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"color": "blue", "size": size},
	}}
}

// sample holds the latencies of one phase.
type sample struct {
	durations []time.Duration
	failures  int
	elapsed   time.Duration
	mu        sync.Mutex
}

func (s *sample) record(d time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		s.failures++
		return
	}
	s.durations = append(s.durations, d)
}

func (s *sample) percentile(p float64) time.Duration {
	if len(s.durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(s.durations))
	copy(sorted, s.durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func (s *sample) throughput() float64 {
	if s.elapsed == 0 {
		return 0
	}
	return float64(len(s.durations)) / s.elapsed.Seconds()
}

// run executes work count times across the configured number of workers.
func run(count int, workers int, work func(i int) error) *sample {
	result := &sample{}
	queue := make(chan int, count)
	for i := range count {
		queue <- i
	}
	close(queue)

	started := time.Now()
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := range queue {
				begin := time.Now()
				err := work(i)
				result.record(time.Since(begin), err)
			}
		}()
	}
	group.Wait()
	result.elapsed = time.Since(started)

	return result
}

func TestBenchmark(t *testing.T) {
	ctx := context.Background()
	cfg := config(t)

	cluster, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("building the clientset: %v", err)
	}

	installNativeCRD(ctx, t, cfg)

	// Everything measured below runs as this subject; installing the CRD and
	// reading the metrics stay on the admin configuration, which is not
	// something the measured workload needs to be able to do.
	dynamicClient, err := dynamic.NewForConfig(workloadConfig(ctx, t, cluster, cfg))
	if err != nil {
		t.Fatalf("building the workload client: %v", err)
	}

	authorizationsBefore := authorizationChecks(ctx, t, cluster)

	// What unrelated traffic looks like when nothing else is happening, so the
	// numbers taken under load have something to be compared with.
	watcher := &bystander{client: cluster, every: 200 * time.Millisecond}
	idle := watcher.watch(ctx, "idle")
	time.Sleep(10 * time.Second)
	unrelated := map[string]*series{"idle": {}, "native CRD": {}, "kcp via spillway": {}}
	unrelated["idle"].add(idle())

	backends := []backend{
		{name: "native CRD", client: dynamicClient.Resource(schema.GroupVersionResource{
			Group: nativeGroup, Version: "v1alpha1", Resource: "widgets"})},
		{name: "kcp via spillway", client: dynamicClient.Resource(schema.GroupVersionResource{
			Group: offloadedGroup, Version: "v1alpha1", Resource: "widgets"})},
	}

	// A discarded pass first. The first requests of a run carry connection
	// setup, discovery, the OpenAPI documents and spillway's informer sync, and
	// measuring those once at the start of one backend and not the other is the
	// kind of bias that shows up as a large run to run swing.
	for _, target := range backends {
		t.Logf("warming up %s with %d objects...", target.name, warmup)
		warmUp(ctx, target)
	}

	storageBefore := storageObjects(ctx, t, cluster)

	rows := []phaseResult{
		{phase: "create", result: map[string]*series{}},
		{phase: "get", result: map[string]*series{}},
		{phase: "list", result: map[string]*series{}},
		{phase: "patch", result: map[string]*series{}},
		{phase: "delete", result: map[string]*series{}},
	}
	watchLatency := map[string]*series{}
	for _, row := range rows {
		for _, target := range backends {
			row.result[target.name] = &series{}
		}
	}
	for _, target := range backends {
		watchLatency[target.name] = &series{}
	}

	names := map[string][]string{}
	for _, target := range backends {
		names[target.name] = make([]string, objects)
		for i := range names[target.name] {
			names[target.name][i] = fmt.Sprintf("bench-%s-%04d", shortName(target), i)
		}
	}

	var storagePeak map[string]int

	for iteration := 1; iteration <= runs; iteration++ {
		t.Logf("run %d of %d", iteration, runs)

		// Create for both before measuring anything else, so the objects are
		// all present when the cluster is asked what it is storing.
		for _, target := range backends {
			widgets := target.client.Namespace(namespace)

			// Sampled during the create phase: three thousand writes is the
			// heaviest thing this does, and where the difference between the
			// cluster storing them and kcp storing them should show.
			bystanders := watcher.watch(ctx, shortName(target))
			rows[0].result[target.name].add(run(objects, concurrency, func(i int) error {
				_, err := widgets.Create(ctx, widget(groupOf(target), names[target.name][i], int64(i%100)+1), metav1.CreateOptions{})
				return err
			}))
			unrelated[target.name].add(bystanders())
		}

		// apiserver_storage_objects is recomputed on a timer, so the count lags
		// the writes that caused it. Once is enough.
		if storagePeak == nil {
			storagePeak = awaitStorageCount(ctx, t, cluster, "widgets."+nativeGroup, objects)
		}

		for _, target := range backends {
			widgets := target.client.Namespace(namespace)

			rows[1].result[target.name].add(run(reads, concurrency, func(i int) error {
				_, err := widgets.Get(ctx, names[target.name][i%objects], metav1.GetOptions{})
				return err
			}))

			rows[2].result[target.name].add(run(lists, concurrency, func(int) error {
				_, err := widgets.List(ctx, metav1.ListOptions{})
				return err
			}))

			rows[3].result[target.name].add(run(reads, concurrency, func(i int) error {
				_, err := widgets.Patch(ctx, names[target.name][i%objects], types.MergePatchType,
					[]byte(`{"spec":{"size":42}}`), metav1.PatchOptions{})
				return err
			}))

			watchLatency[target.name].add(measureWatch(ctx, t, widgets, groupOf(target), shortName(target)))
		}

		for _, target := range backends {
			widgets := target.client.Namespace(namespace)
			rows[4].result[target.name].add(run(objects, concurrency, func(i int) error {
				return widgets.Delete(ctx, names[target.name][i], metav1.DeleteOptions{})
			}))
		}
	}

	authorizations := authorizationChecks(ctx, t, cluster) - authorizationsBefore

	report(t, backends, rows, watchLatency, storageBefore, storagePeak, authorizations, unrelated)
}

// warmUp exercises every verb the benchmark measures and throws the results
// away. The objects it creates are deleted again, so they do not appear in the
// storage count taken afterwards.
func warmUp(ctx context.Context, target backend) {
	widgets := target.client.Namespace(namespace)
	names := make([]string, warmup)
	for i := range names {
		names[i] = fmt.Sprintf("warmup-%s-%04d", shortName(target), i)
	}

	run(warmup, concurrency, func(i int) error {
		_, err := widgets.Create(ctx, widget(groupOf(target), names[i], int64(i%100)+1), metav1.CreateOptions{})
		return err
	})
	run(warmup, concurrency, func(i int) error {
		_, err := widgets.Get(ctx, names[i], metav1.GetOptions{})
		return err
	})
	run(warmup, concurrency, func(i int) error {
		_, err := widgets.Patch(ctx, names[i], types.MergePatchType,
			[]byte(`{"spec":{"size":7}}`), metav1.PatchOptions{})
		return err
	})
	run(5, 5, func(int) error {
		_, err := widgets.List(ctx, metav1.ListOptions{})
		return err
	})
	run(warmup, concurrency, func(i int) error {
		return widgets.Delete(ctx, names[i], metav1.DeleteOptions{})
	})
}

// workloadConfig returns the identity the measured traffic uses. Anything in
// system:masters never reaches spillway's authorizer, so an admin measures a
// path no ordinary client takes.
func workloadConfig(ctx context.Context, t *testing.T, cluster kubernetes.Interface, admin *rest.Config) *rest.Config {
	t.Helper()

	if !asNormalUser {
		t.Log("measuring as the admin identity: authorization is short-circuited for system:masters")
		return admin
	}

	const name = "bench-user"
	if _, err := cluster.CoreV1().ServiceAccounts(namespace).Create(ctx,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}},
		metav1.CreateOptions{}); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("creating the benchmark service account: %v", err)
	}

	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{nativeGroup, offloadedGroup},
			Resources: []string{"widgets", "widgets/status"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		}},
	}
	if _, err := cluster.RbacV1().ClusterRoles().Update(ctx, role, metav1.UpdateOptions{}); err != nil {
		if _, err := cluster.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			t.Fatalf("creating the benchmark role: %v", err)
		}
	}

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: namespace}},
	}
	if _, err := cluster.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("binding the benchmark role: %v", err)
	}

	hour := int64(3600)
	token, err := cluster.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, name,
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &hour}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("minting a token for the benchmark subject: %v", err)
	}

	config := rest.CopyConfig(admin)
	config.BearerToken = token.Status.Token
	config.BearerTokenFile = ""
	// The admin credentials would otherwise win.
	config.CertFile, config.KeyFile = "", ""
	config.CertData, config.KeyData = nil, nil
	config.Username, config.Password = "", ""

	t.Logf("measuring as system:serviceaccount:%s:%s", namespace, name)
	return config
}

// authorizationChecks counts the SubjectAccessReviews the cluster has served,
// which is what spillway asks it per request that its cache has not seen.
func authorizationChecks(ctx context.Context, t *testing.T, cluster kubernetes.Interface) int {
	t.Helper()

	raw, err := cluster.Discovery().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		return 0
	}

	var total int
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "apiserver_request_total{") || !strings.Contains(line, `resource="subjectaccessreviews"`) {
			continue
		}
		value := line[strings.LastIndex(line, " ")+1:]
		if count, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			total += int(count)
		}
	}
	return total
}

func shortName(b backend) string {
	if b.name == "native CRD" {
		return "native"
	}
	return "kcp"
}

// awaitStorageCount waits for the cluster's storage metric to catch up with the
// objects just written, so the comparison is made at the peak rather than after
// the metric has been recomputed.
func awaitStorageCount(ctx context.Context, t *testing.T, cluster kubernetes.Interface, resource string, want int) map[string]int {
	t.Helper()

	var latest map[string]int
	deadline := time.After(3 * time.Minute)
	for {
		latest = storageObjects(ctx, t, cluster)
		if latest[resource] >= want {
			return latest
		}
		select {
		case <-deadline:
			t.Logf("the cluster still reports %d %s after waiting; reporting what it says",
				latest[resource], resource)
			return latest
		case <-time.After(5 * time.Second):
		}
	}
}

func groupOf(b backend) string {
	if b.name == "native CRD" {
		return nativeGroup
	}
	return offloadedGroup
}

// measureWatch reports how long an event takes to reach a watcher after the
// write that caused it returned.
func measureWatch(ctx context.Context, t *testing.T, widgets dynamic.ResourceInterface, group, prefix string) *sample {
	t.Helper()

	result := &sample{}

	list, err := widgets.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing before the watch: %v", err)
	}
	watcher, err := widgets.Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
	if err != nil {
		t.Fatalf("starting the watch: %v", err)
	}
	defer watcher.Stop()

	seen := make(chan string, watches*4)
	go func() {
		for event := range watcher.ResultChan() {
			if object, ok := event.Object.(*unstructured.Unstructured); ok {
				seen <- object.GetName()
			}
		}
	}()

	started := time.Now()
	for i := range watches {
		name := fmt.Sprintf("watch-%s-%03d", strings.Split(prefix, "-")[0], i)
		begin := time.Now()
		if _, err := widgets.Create(ctx, widget(group, name, 1), metav1.CreateOptions{}); err != nil {
			result.record(0, err)
			continue
		}

		deadline := time.After(30 * time.Second)
		for {
			select {
			case got := <-seen:
				if got != name {
					continue
				}
				result.record(time.Since(begin), nil)
			case <-deadline:
				result.record(0, fmt.Errorf("no event for %s", name))
			}
			break
		}
	}
	result.elapsed = time.Since(started)

	for i := range watches {
		_ = widgets.Delete(ctx, fmt.Sprintf("watch-%s-%03d", strings.Split(prefix, "-")[0], i), metav1.DeleteOptions{})
	}
	return result
}

// storageObjects reads what the cluster's own apiserver reports it is storing,
// which is the number the whole exercise is about.
func storageObjects(ctx context.Context, t *testing.T, cluster kubernetes.Interface) map[string]int {
	t.Helper()

	raw, err := cluster.Discovery().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		t.Logf("could not read the cluster's metrics: %v", err)
		return nil
	}

	counts := map[string]int{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "apiserver_storage_objects{") {
			continue
		}
		resource := between(line, `resource="`, `"`)
		value := line[strings.LastIndex(line, " ")+1:]
		if count, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			counts[resource] = int(count)
		}
	}
	return counts
}

func between(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return ""
	}
	rest := s[i+len(start):]
	j := strings.Index(rest, end)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func installNativeCRD(ctx context.Context, t *testing.T, cfg *rest.Config) {
	t.Helper()

	client, err := apiextensionsclient.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("building the apiextensions client: %v", err)
	}

	name := "widgets." + nativeGroup
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: nativeGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind: "Widget", ListKind: "WidgetList", Plural: "widgets", Singular: "widget",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					// The same shape the offloaded CRD has, so the comparison is
					// between where the object lives and nothing else.
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type:     "object",
								Required: []string{"color"},
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"color": {Type: "string", Enum: []apiextensionsv1.JSON{
										{Raw: []byte(`"red"`)}, {Raw: []byte(`"green"`)}, {Raw: []byte(`"blue"`)},
									}},
									"size": {Type: "integer", Minimum: ptr(1.0), Maximum: ptr(100.0)},
								},
							},
							"status": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"phase": {Type: "string"},
							}},
						},
					},
				},
			}},
		},
	}

	if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("installing the native CRD: %v", err)
		}
	}

	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			current, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			for _, condition := range current.Status.Conditions {
				if condition.Type == apiextensionsv1.Established {
					return condition.Status == apiextensionsv1.ConditionTrue, nil
				}
			}
			return false, nil
		}); err != nil {
		t.Fatalf("the native CRD never became Established: %v", err)
	}
}

func ptr(f float64) *float64 { return &f }
