package apiserver

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/mrueg/spillway/pkg/kcp"
)

func testWorkspace(t *testing.T, name string, patterns []string, versions ...string) *workspace {
	t.Helper()

	matcher, err := kcp.ParseGroupMatcher(patterns)
	if err != nil {
		t.Fatalf("parsing %v: %v", patterns, err)
	}

	// The stub serves registrarGroup; a workspace whose matcher does not cover
	// it ends up with an empty snapshot, which is what a workspace backing a
	// group the workspace has not been given looks like.
	cache := kcp.NewResourceCache("test", &registrarDiscovery{versions: versions}, matcher)
	if err := cache.Refresh(); err != nil {
		t.Fatalf("priming %s: %v", name, err)
	}
	return &workspace{name: name, groups: matcher, cache: cache}
}

// The point of the router: a group is answered by the workspace that backs it,
// and the merged view is what discovery reads.
func TestRouterMergesWorkspaces(t *testing.T) {
	first := testWorkspace(t, "first", []string{registrarGroup}, "v1alpha1")
	second := testWorkspace(t, "second", []string{"*.other.example.net"}, "v1alpha1")

	routes := newRouter([]*workspace{first, second})

	if got := routes.ServedGroups(); !got.Equal(sets.New(registrarGroup)) {
		t.Errorf("served groups = %v, want just the one the first workspace backs", sets.List(got))
	}
	if owner := routes.servingFor(registrarGroup); owner != first {
		t.Errorf("servingFor(%s) = %v, want the first workspace", registrarGroup, owner)
	}
	if routes.servingFor("nobody.example.com") != nil {
		t.Error("a group no workspace serves resolved to one")
	}

	// Configured to serve is not the same as currently serving: the second
	// workspace owns its domain whether or not the workspace has any yet.
	if !routes.Owns("anything.other.example.net") {
		t.Error("the second workspace's wildcard is not owned")
	}
	if routes.Owns("apps") {
		t.Error("a group nobody configured is owned")
	}
}

// Two workspaces whose wildcards overlap cannot both serve one group: there is
// one APIService for it, pointing at one spillway, which has to pick.
func TestRouterGivesAContestedGroupToTheFirstWorkspace(t *testing.T) {
	first := testWorkspace(t, "first", []string{"*.example.com"}, "v1alpha1")
	second := testWorkspace(t, "second", []string{"*.example.com"}, "v1alpha1")

	routes := newRouter([]*workspace{first, second})

	if owner := routes.servingFor(registrarGroup); owner != first {
		t.Errorf("servingFor(%s) = %v, want the first workspace configured for it", registrarGroup, owner)
	}
	snapshot := routes.Snapshot()
	if len(snapshot.Resources) != 1 {
		t.Errorf("merged resources = %v, want one workspace's copy of the group", snapshot.Resources)
	}
}

// The merged snapshot is read on every discovery request, so it is cached --
// and has to notice when a workspace moves on.
func TestRouterRebuildsWhenAWorkspaceChanges(t *testing.T) {
	matcher, err := kcp.ParseGroupMatcher([]string{registrarGroup})
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	workspaceDiscovery := &registrarDiscovery{versions: []string{"v1alpha1"}}
	cache := kcp.NewResourceCache("test", workspaceDiscovery, matcher)
	if err := cache.Refresh(); err != nil {
		t.Fatalf("priming: %v", err)
	}

	only := &workspace{name: "only", groups: matcher, cache: cache}
	routes := newRouter([]*workspace{only})

	before := routes.Snapshot()
	if _, found := before.Resources[schema.GroupVersion{Group: registrarGroup, Version: "v1beta1"}]; found {
		t.Fatal("the workspace already serves v1beta1")
	}
	if again := routes.Snapshot(); again != before {
		t.Error("the merged snapshot was rebuilt although nothing changed")
	}

	// The workspace gains a version, as it would from a CRD appearing in it.
	workspaceDiscovery.versions = []string{"v1alpha1", "v1beta1"}
	if err := cache.Refresh(); err != nil {
		t.Fatalf("refreshing: %v", err)
	}

	after := routes.Snapshot()
	if _, found := after.Resources[schema.GroupVersion{Group: registrarGroup, Version: "v1beta1"}]; !found {
		t.Error("a version added to a workspace did not reach the merged snapshot")
	}
}

// Reporting synced before every workspace has been read would describe a
// missing workspace as one that serves nothing.
func TestRouterIsNotSyncedUntilEveryWorkspaceIs(t *testing.T) {
	synced := testWorkspace(t, "synced", []string{registrarGroup}, "v1alpha1")

	matcher, err := kcp.ParseGroupMatcher([]string{"*.other.example.net"})
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	unsynced := &workspace{
		name:   "unsynced",
		groups: matcher,
		cache:  kcp.NewResourceCache("test", &registrarDiscovery{versions: []string{"v1alpha1"}}, matcher),
	}

	routes := newRouter([]*workspace{synced, unsynced})
	if routes.HasSynced() {
		t.Error("the router reported synced with a workspace that has never been read")
	}
}
