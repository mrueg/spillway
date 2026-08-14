package apiserver

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorfake "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/fake"

	"github.com/mrueg/spillway/pkg/kcp"
)

const registrarGroup = "spillway.example.com"

// registrarDiscovery serves one group at the given versions, which is all the
// resource cache reads.
type registrarDiscovery struct {
	discovery.DiscoveryInterface

	versions []string
}

func (d *registrarDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	group := metav1.APIGroup{Name: registrarGroup}
	for _, version := range d.versions {
		group.Versions = append(group.Versions, metav1.GroupVersionForDiscovery{
			GroupVersion: registrarGroup + "/" + version, Version: version,
		})
	}
	return &metav1.APIGroupList{Groups: []metav1.APIGroup{group}}, nil
}

func (d *registrarDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return &metav1.APIResourceList{
		GroupVersion: groupVersion,
		APIResources: []metav1.APIResource{{Name: "widgets", Kind: "Widget", Namespaced: true}},
	}, nil
}

func registrarMatcher(t *testing.T) *kcp.GroupMatcher {
	t.Helper()

	matcher, err := kcp.ParseGroupMatcher([]string{registrarGroup})
	if err != nil {
		t.Fatalf("parsing the group matcher: %v", err)
	}
	return matcher
}

func cacheFor(t *testing.T, versions ...string) *kcp.ResourceCache {
	t.Helper()

	cache := kcp.NewResourceCache("test", &registrarDiscovery{versions: versions}, registrarMatcher(t))
	if err := cache.Refresh(); err != nil {
		t.Fatalf("priming the resource cache: %v", err)
	}
	return cache
}

func testRegistrar(t *testing.T, versions []string, existing ...*apiregistrationv1.APIService) (*apiServiceRegistrar, *aggregatorfake.Clientset) {
	t.Helper()

	objects := make([]runtime.Object, 0, len(existing))
	for _, service := range existing {
		objects = append(objects, service)
	}
	client := aggregatorfake.NewSimpleClientset(objects...)

	return &apiServiceRegistrar{
		client: client,
		cache:  cacheFor(t, versions...),
		options: APIServiceOptions{
			Register:              true,
			ServiceName:           "spillway",
			ServiceNamespace:      "spillway-system",
			ServicePort:           443,
			InsecureSkipTLSVerify: true,
			GroupPriorityMinimum:  1000,
			VersionPriority:       15,
		},
	}, client
}

func apiServiceNames(t *testing.T, client *aggregatorfake.Clientset) sets.Set[string] {
	t.Helper()

	list, err := client.ApiregistrationV1().APIServices().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing APIServices: %v", err)
	}
	names := sets.New[string]()
	for _, item := range list.Items {
		names.Insert(item.Name)
	}
	return names
}

// The point of the feature: what the workspace serves is what gets registered,
// including a version no manifest ever mentioned.
func TestRegistrarRegistersEveryServedVersion(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1", "v1beta1"})

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	want := sets.New("v1alpha1."+registrarGroup, "v1beta1."+registrarGroup)
	if got := apiServiceNames(t, client); !got.Equal(want) {
		t.Errorf("registered %v, want %v", sets.List(got), sets.List(want))
	}

	created, err := client.ApiregistrationV1().APIServices().Get(context.Background(),
		"v1beta1."+registrarGroup, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the created APIService: %v", err)
	}
	if created.Labels[managedByLabel] != managedByValue {
		t.Errorf("labels = %v, want %s=%s", created.Labels, managedByLabel, managedByValue)
	}
	if created.Spec.Service == nil || created.Spec.Service.Name != "spillway" ||
		created.Spec.Service.Namespace != "spillway-system" {
		t.Errorf("service reference = %+v, want spillway-system/spillway", created.Spec.Service)
	}
	if created.Spec.GroupPriorityMinimum != 1000 || created.Spec.VersionPriority != 15 {
		t.Errorf("priorities = %d/%d, want 1000/15", created.Spec.GroupPriorityMinimum, created.Spec.VersionPriority)
	}
}

// An APIService somebody declared is not spillway's to rewrite, even when it
// names a group spillway serves.
func TestRegistrarLeavesUnlabelledAPIServicesAlone(t *testing.T) {
	declared := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1alpha1." + registrarGroup},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                registrarGroup,
			Version:              "v1alpha1",
			GroupPriorityMinimum: 17,
			VersionPriority:      3,
		},
	}
	registrar, client := testRegistrar(t, []string{"v1alpha1"}, declared)

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	after, err := client.ApiregistrationV1().APIServices().Get(context.Background(),
		"v1alpha1."+registrarGroup, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the declared APIService: %v", err)
	}
	if after.Spec.GroupPriorityMinimum != 17 || after.Spec.VersionPriority != 3 {
		t.Errorf("priorities = %d/%d, want the declared 17/3 untouched",
			after.Spec.GroupPriorityMinimum, after.Spec.VersionPriority)
	}
	if _, labelled := after.Labels[managedByLabel]; labelled {
		t.Error("spillway claimed an APIService it did not create")
	}
}

// A version the workspace stopped serving leaves an APIService that fails the
// availability probe, which degrades discovery cluster wide. Spillway withdraws
// the ones it created.
func TestRegistrarWithdrawsVersionsTheWorkspaceDropped(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1", "v1beta1"})
	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	registrar.cache = cacheFor(t, "v1alpha1")
	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}

	want := sets.New("v1alpha1." + registrarGroup)
	if got := apiServiceNames(t, client); !got.Equal(want) {
		t.Errorf("after the workspace dropped v1beta1, registered %v, want %v", sets.List(got), sets.List(want))
	}
}

// A declared APIService for a version that went away stays: withdrawing it is
// the declarer's decision, and spillway removing it would fight whatever
// applied it.
func TestRegistrarDoesNotWithdrawWhatItDidNotRegister(t *testing.T) {
	declared := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1beta1." + registrarGroup},
		Spec:       apiregistrationv1.APIServiceSpec{Group: registrarGroup, Version: "v1beta1"},
	}
	registrar, client := testRegistrar(t, []string{"v1alpha1"}, declared)

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if got := apiServiceNames(t, client); !got.Has("v1beta1." + registrarGroup) {
		t.Errorf("registered %v: spillway removed an APIService it did not create", sets.List(got))
	}
}

// The label alone must not be enough to make spillway delete something: it also
// has to be one of spillway's own groups.
func TestRegistrarIgnoresLabelledAPIServicesForOtherGroups(t *testing.T) {
	mislabelled := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "v1.metrics.k8s.io",
			Labels: map[string]string{managedByLabel: managedByValue},
		},
		Spec: apiregistrationv1.APIServiceSpec{Group: "metrics.k8s.io", Version: "v1"},
	}
	registrar, client := testRegistrar(t, []string{"v1alpha1"}, mislabelled)

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if got := apiServiceNames(t, client); !got.Has("v1.metrics.k8s.io") {
		t.Errorf("registered %v: spillway deleted another component's APIService", sets.List(got))
	}
}

// An unsynced cache reports no group versions, which must not be read as "the
// workspace serves nothing".
func TestRegistrarDoesNothingUntilTheCacheHasSynced(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1"})
	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	// A fresh cache that has never refreshed: empty, and not to be trusted.
	registrar.cache = kcp.NewResourceCache("test", &registrarDiscovery{versions: []string{"v1alpha1"}}, registrarMatcher(t))
	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync with an unsynced cache: %v", err)
	}

	if got := apiServiceNames(t, client); !got.Has("v1alpha1." + registrarGroup) {
		t.Errorf("registered %v: an unsynced cache withdrew a live APIService", sets.List(got))
	}
}

// The count published as spillway_apiservice_managed has to be what spillway
// actually maintains, not everything it serves: an APIService somebody else
// declared is not the registrar's responsibility.
func TestRegistrarCountsOnlyWhatItManages(t *testing.T) {
	declared := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1alpha1." + registrarGroup},
		Spec:       apiregistrationv1.APIServiceSpec{Group: registrarGroup, Version: "v1alpha1"},
	}
	registrar, _ := testRegistrar(t, []string{"v1alpha1", "v1beta1"}, declared)

	managed, err := registrar.reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if managed != 1 {
		t.Errorf("managed = %d, want 1: v1beta1 is spillway's, v1alpha1 was declared", managed)
	}

	// Steady state: the same answer, without double counting on a second pass.
	managed, err = registrar.reconcile(context.Background())
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if managed != 1 {
		t.Errorf("managed = %d on the second pass, want 1", managed)
	}
}

// The guard, from the registrar's side: a group version the cluster serves
// itself must not get an APIService pointing at spillway. Measured on a live
// cluster, doing so leaves the group's entry in aggregated discovery with an
// empty resource list that does not repopulate -- kubectl stops seeing the
// resource while the objects sit untouched in etcd.
func TestRegistrarWillNotRegisterOverTheCluster(t *testing.T) {
	// One clientset for both: in a running cluster the registrar writes to the
	// same API it reads the routing from.
	local := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1alpha1." + registrarGroup},
		Spec:       apiregistrationv1.APIServiceSpec{Group: registrarGroup, Version: "v1alpha1"},
	}
	registrar, client := testRegistrar(t, []string{"v1alpha1"}, local)
	registrar.cluster = &clusterAPIs{
		discovery:  &clusterDiscovery{groups: []string{registrarGroup}, version: "v1alpha1"},
		aggregator: client,
		service:    spillwayService(),
	}
	registrar.warned = sets.New[schema.GroupVersion]()

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	found, err := client.ApiregistrationV1().APIServices().Get(context.Background(),
		"v1alpha1."+registrarGroup, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the APIService: %v", err)
	}
	if found.Spec.Service != nil {
		t.Errorf("spillway pointed the cluster's own group at itself: %+v", found.Spec.Service)
	}
	if found.Labels[managedByLabel] == managedByValue {
		t.Error("spillway claimed the cluster's own APIService")
	}
}

// If the cluster cannot be asked, nothing is registered: a wrong yes hides
// another API silently, a wrong no leaves a group unserved and visible.
func TestRegistrarRefusesToActWithoutKnowing(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1"})
	registrar.cluster = &clusterAPIs{
		discovery:  &clusterDiscovery{err: errors.New("apiserver unreachable")},
		aggregator: aggregatorfake.NewSimpleClientset(),
		service:    spillwayService(),
	}

	if err := registrar.sync(context.Background()); err == nil {
		t.Error("sync succeeded without being able to check what the cluster serves")
	}
	if names := apiServiceNames(t, client); names.Len() != 0 {
		t.Errorf("registered %v despite not knowing what the cluster serves", sets.List(names))
	}
}

// A workspace taken out of the configuration takes its groups with it, so the
// APIServices spillway registered for them have to be withdrawn -- which an
// ownership test based on the group could never do, since the group stops being
// owned at exactly the moment the withdrawal is needed.
func TestRegistrarWithdrawsWhenTheGroupStopsBeingConfigured(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1"})
	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if got := apiServiceNames(t, client); !got.Has("v1alpha1." + registrarGroup) {
		t.Fatalf("registered %v, want the group", sets.List(got))
	}

	// The workspace backing it is gone: nothing is configured any more.
	empty, err := kcp.ParseGroupMatcher([]string{"nothing.example.net"})
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	gone := kcp.NewResourceCache("test", &registrarDiscovery{versions: nil}, empty)
	if err := gone.Refresh(); err != nil {
		t.Fatalf("refreshing: %v", err)
	}
	registrar.cache = gone

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if got := apiServiceNames(t, client); got.Len() != 0 {
		t.Errorf("registered %v, want the withdrawal to have happened", sets.List(got))
	}
}

// A replica that is not leading must not write, and must not report that it is
// maintaining anything.
func TestRegistrarWritesNothingWhenNotLeading(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1"})
	registrar.leader = &leadership{} // constructed, never elected

	managed, err := registrar.reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if managed != 0 {
		t.Errorf("managed = %d while not leading, want 0", managed)
	}
	if names := apiServiceNames(t, client); names.Len() != 0 {
		t.Errorf("registered %v while not leading", sets.List(names))
	}

	// And when it is elected, it does the work it had been leaving alone.
	registrar.leader.leading.Store(true)
	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync after election: %v", err)
	}
	if names := apiServiceNames(t, client); !names.Has("v1alpha1." + registrarGroup) {
		t.Errorf("registered %v after being elected, want the group", sets.List(names))
	}
}

// No election configured is the single replica case, and the one where the
// operator did not ask: both want every replica to register.
func TestRegistrarWithoutAnElectionLeads(t *testing.T) {
	registrar, client := testRegistrar(t, []string{"v1alpha1"})
	registrar.leader = nil

	if err := registrar.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if names := apiServiceNames(t, client); !names.Has("v1alpha1." + registrarGroup) {
		t.Errorf("registered %v, want the group", sets.List(names))
	}
}
