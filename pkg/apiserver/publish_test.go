package apiserver

import (
	"slices"
	"testing"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/mrueg/spillway/pkg/kcp"
)

type fakeGroupManager struct {
	added   map[string]metav1.APIGroup
	removed []string
}

func newFakeGroupManager() *fakeGroupManager {
	return &fakeGroupManager{added: map[string]metav1.APIGroup{}}
}

func (f *fakeGroupManager) AddGroup(apiGroup metav1.APIGroup) { f.added[apiGroup.Name] = apiGroup }
func (f *fakeGroupManager) RemoveGroup(groupName string) {
	delete(f.added, groupName)
	f.removed = append(f.removed, groupName)
}

type fakeAggregatedManager struct {
	added      map[string]apidiscoveryv2.APIVersionDiscovery
	priorities map[metav1.GroupVersion][2]int
	removed    []string
}

func newFakeAggregatedManager() *fakeAggregatedManager {
	return &fakeAggregatedManager{
		added:      map[string]apidiscoveryv2.APIVersionDiscovery{},
		priorities: map[metav1.GroupVersion][2]int{},
	}
}

func (f *fakeAggregatedManager) AddGroupVersion(groupName string, value apidiscoveryv2.APIVersionDiscovery) {
	f.added[groupName+"/"+value.Version] = value
}

func (f *fakeAggregatedManager) SetGroupVersionPriority(gv metav1.GroupVersion, group, version int) {
	f.priorities[gv] = [2]int{group, version}
}

func (f *fakeAggregatedManager) RemoveGroup(groupName string) {
	f.removed = append(f.removed, groupName)
}

func newPublishHandler(snapshot *kcp.Snapshot) (*discoveryHandler, *fakeGroupManager, *fakeAggregatedManager) {
	groups, aggregated := newFakeGroupManager(), newFakeAggregatedManager()
	return &discoveryHandler{
		cache:      fakeSnapshotter{snapshot: snapshot},
		discovery:  groups,
		aggregated: aggregated,
	}, groups, aggregated
}

func TestPublishAdvertisesTheGroup(t *testing.T) {
	handler, groups, aggregated := newPublishHandler(widgetSnapshot())

	handler.publish()

	published, found := groups.added[testGroup]
	if !found {
		t.Fatalf("group %s was not added to /apis: %v", testGroup, groups.added)
	}
	if published.PreferredVersion.Version != "v1alpha1" {
		t.Errorf("PreferredVersion = %q, want v1alpha1", published.PreferredVersion.Version)
	}

	version, found := aggregated.added[testGroup+"/v1alpha1"]
	if !found {
		t.Fatalf("group version was not added to aggregated discovery: %v", aggregated.added)
	}
	if len(version.Resources) != 1 || version.Resources[0].Resource != "widgets" {
		t.Errorf("aggregated resources = %+v, want a single widgets entry", version.Resources)
	}
}

// Without an explicit priority the aggregated document is ordered by map
// iteration, so the same workspace can produce a different answer per call.
func TestPublishSetsAPriority(t *testing.T) {
	handler, _, aggregated := newPublishHandler(widgetSnapshot())

	handler.publish()

	gv := metav1.GroupVersion{Group: testGroup, Version: "v1alpha1"}
	priority, found := aggregated.priorities[gv]
	if !found {
		t.Fatalf("no priority was set for %s: %v", gv, aggregated.priorities)
	}
	if priority != [2]int{discoveryGroupPriority, discoveryVersionPriority} {
		t.Errorf("priority = %v, want {%d %d}", priority, discoveryGroupPriority, discoveryVersionPriority)
	}
}

// A group the workspace has stopped serving must be withdrawn, or clients keep
// being pointed at resources that are gone.
func TestPublishWithdrawsAGroupTheWorkspaceDropped(t *testing.T) {
	handler, groups, aggregated := newPublishHandler(widgetSnapshot())
	handler.publish()

	if _, found := groups.added[testGroup]; !found {
		t.Fatal("precondition: the group should be published first")
	}

	handler.cache = fakeSnapshotter{snapshot: &kcp.Snapshot{Groups: map[string]metav1.APIGroup{}}}
	handler.publish()

	if _, found := groups.added[testGroup]; found {
		t.Error("the group is still advertised in /apis after the workspace dropped it")
	}
	if len(groups.removed) != 1 || groups.removed[0] != testGroup {
		t.Errorf("RemoveGroup calls = %v, want [%s]", groups.removed, testGroup)
	}
	if len(aggregated.removed) != 1 || aggregated.removed[0] != testGroup {
		t.Errorf("aggregated RemoveGroup calls = %v, want [%s]", aggregated.removed, testGroup)
	}
}

// Aggregated discovery is optional on the generic server, so publish has to
// cope with it being absent rather than panicking on the first refresh.
func TestPublishWithoutAggregatedDiscovery(t *testing.T) {
	groups := newFakeGroupManager()
	handler := &discoveryHandler{
		cache:     fakeSnapshotter{snapshot: widgetSnapshot()},
		discovery: groups,
	}

	handler.publish()

	if _, found := groups.added[testGroup]; !found {
		t.Error("the group was not added to /apis when aggregated discovery is disabled")
	}
}

// Before the first successful sync there is nothing to advertise, and a group
// must not be published empty.
func TestPublishBeforeTheFirstSync(t *testing.T) {
	handler, groups, _ := newPublishHandler(&kcp.Snapshot{
		Groups: map[string]metav1.APIGroup{},
	})

	handler.publish()

	if len(groups.added) != 0 {
		t.Errorf("published %v before any sync, want nothing", groups.added)
	}
}

// Publishing is driven by the snapshot rather than by a configured list, so a
// group that arrives through a wildcard is advertised and one that disappears
// is withdrawn again.
func TestPublishFollowsTheSnapshot(t *testing.T) {
	const late = "gadgets.tenant.example.net"

	snapshot := widgetSnapshot()
	snapshot.Groups[late] = metav1.APIGroup{
		Name:     late,
		Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: late + "/v1", Version: "v1"}},
	}

	handler, groups, aggregated := newPublishHandler(snapshot)
	handler.publish()

	if _, found := groups.added[late]; !found {
		t.Fatalf("a group discovered through a wildcard was not published: %v", groups.added)
	}

	// It goes away again, and the advertisement has to go with it.
	delete(snapshot.Groups, late)
	handler.publish()

	if !slices.Contains(groups.removed, late) {
		t.Errorf("the group was not withdrawn from /apis: removed = %v", groups.removed)
	}
	if !slices.Contains(aggregated.removed, late) {
		t.Errorf("the group was not withdrawn from aggregated discovery: removed = %v", aggregated.removed)
	}
	if _, found := groups.added[testGroup]; !found {
		t.Error("the group that is still served was withdrawn too")
	}
}
