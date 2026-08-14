package apiserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/mrueg/spillway/pkg/kcp"
)

// testManager builds a manager whose workspaces are stubs: no kcp, no watch,
// just the bookkeeping the reload is made of.
func testManager(t *testing.T, configured ...WorkspaceConfig) (*workspaceManager, *int) {
	t.Helper()

	built := 0
	manager := &workspaceManager{
		router: newRouter(nil),
		build: func(entry WorkspaceConfig) (*workspace, error) {
			built++
			if entry.Kubeconfig == "broken" {
				return nil, errors.New("cannot reach this workspace")
			}
			return &workspace{
				name:   entry.Name,
				groups: entry.APIGroups,
				cache:  kcp.NewResourceCache(entry.Name, &registrarDiscovery{versions: []string{"v1alpha1"}}, entry.APIGroups),
				proxy:  &resourceProxy{credentials: &credentialSource{}},
			}, nil
		},
		watch: func(context.Context, *workspace) (<-chan struct{}, error) { return nil, nil },
		// Zero so the goroutines the launch starts return at once.
		resyncPeriod:     time.Hour,
		credentialReload: 0,
		onChange:         func() {},
	}

	if err := manager.start(context.Background(), configured); err != nil {
		t.Fatalf("start: %v", err)
	}
	return manager, &built
}

func entry(t *testing.T, name, kubeconfig string, groups ...string) WorkspaceConfig {
	t.Helper()

	matcher, err := kcp.ParseGroupMatcher(groups)
	if err != nil {
		t.Fatalf("parsing %v: %v", groups, err)
	}
	return WorkspaceConfig{Name: name, Kubeconfig: kubeconfig, APIGroups: matcher}
}

func names(workspaces []*workspace) sets.Set[string] {
	found := sets.New[string]()
	for _, w := range workspaces {
		found.Insert(w.name)
	}
	return found
}

// A workspace added to the configuration has to start serving without the
// restart that would drop every watch spillway is proxying.
func TestReloadAddsAWorkspace(t *testing.T) {
	first := entry(t, "first", "/a", "a.example.com")
	manager, _ := testManager(t, first)

	second := entry(t, "second", "/b", "b.example.com")
	manager.reload = func() ([]WorkspaceConfig, error) { return []WorkspaceConfig{first, second}, nil }

	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := names(manager.router.snapshot()); !got.Equal(sets.New("first", "second")) {
		t.Errorf("running %v, want both", sets.List(got))
	}
	if !manager.router.Owns("b.example.com") {
		t.Error("the added workspace's groups are not owned")
	}
}

// One taken out of the configuration has to stop, and stop being advertised.
func TestReloadRemovesAWorkspace(t *testing.T) {
	first := entry(t, "first", "/a", "a.example.com")
	second := entry(t, "second", "/b", "b.example.com")
	manager, _ := testManager(t, first, second)

	removed := manager.router.snapshot()[1]
	manager.reload = func() ([]WorkspaceConfig, error) { return []WorkspaceConfig{first}, nil }

	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := names(manager.router.snapshot()); !got.Equal(sets.New("first")) {
		t.Errorf("running %v, want just the first", sets.List(got))
	}
	if manager.router.Owns("b.example.com") {
		t.Error("the removed workspace's groups are still owned")
	}
	if removed.cancel == nil {
		t.Error("the removed workspace was never started")
	}
}

// An unchanged workspace must not be rebuilt: doing so would drop its discovery
// cache and its circuit breaker's state to apply a change somewhere else.
func TestReloadLeavesUnchangedWorkspacesAlone(t *testing.T) {
	first := entry(t, "first", "/a", "a.example.com")
	manager, built := testManager(t, first)

	before := manager.router.snapshot()[0]
	*built = 0

	second := entry(t, "second", "/b", "b.example.com")
	manager.reload = func() ([]WorkspaceConfig, error) { return []WorkspaceConfig{first, second}, nil }
	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if *built != 1 {
		t.Errorf("built %d workspaces, want only the new one", *built)
	}
	for _, running := range manager.router.snapshot() {
		if running.name == "first" && running != before {
			t.Error("the unchanged workspace was rebuilt")
		}
	}
}

// A workspace whose kubeconfig or groups changed is a different workspace: what
// it was built from is what its cache and proxy were built from.
func TestReloadReplacesAChangedWorkspace(t *testing.T) {
	first := entry(t, "first", "/a", "a.example.com")
	manager, _ := testManager(t, first)
	before := manager.router.snapshot()[0]

	moved := entry(t, "first", "/somewhere-else", "a.example.com")
	manager.reload = func() ([]WorkspaceConfig, error) { return []WorkspaceConfig{moved}, nil }
	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if manager.router.snapshot()[0] == before {
		t.Error("a workspace whose kubeconfig changed was left as it was")
	}
}

// An entry that cannot be brought up must not take the previous one down with
// it: the configuration is aspirational, what is running is serving.
func TestReloadKeepsTheOldOneWhenTheNewOneFails(t *testing.T) {
	first := entry(t, "first", "/a", "a.example.com")
	manager, _ := testManager(t, first)
	before := manager.router.snapshot()[0]

	broken := entry(t, "first", "broken", "a.example.com")
	manager.reload = func() ([]WorkspaceConfig, error) { return []WorkspaceConfig{broken}, nil }
	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	running := manager.router.snapshot()
	if len(running) != 1 || running[0] != before {
		t.Errorf("running %v, want the previous workspace still serving", names(running))
	}
}

// A configuration that cannot be read at all changes nothing.
func TestReloadThatCannotBeReadChangesNothing(t *testing.T) {
	first := entry(t, "first", "/a", "a.example.com")
	manager, _ := testManager(t, first)

	manager.reload = func() ([]WorkspaceConfig, error) { return nil, errors.New("no such file") }
	if err := manager.reconcile(context.Background()); err == nil {
		t.Error("an unreadable configuration was applied")
	}
	if got := names(manager.router.snapshot()); !got.Equal(sets.New("first")) {
		t.Errorf("running %v, want it untouched", sets.List(got))
	}
}

// Removing a workspace lowers the sum of the generations, so a merged snapshot
// keyed on that alone would serve the old set from cache.
func TestRouterNoticesTheSetChanging(t *testing.T) {
	first := entry(t, "first", "/a", registrarGroup)
	manager, _ := testManager(t, first)

	if got := manager.router.ServedGroups(); !got.Has(registrarGroup) {
		t.Fatalf("served %v, want the group", sets.List(got))
	}

	manager.reload = func() ([]WorkspaceConfig, error) { return nil, nil }
	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := manager.router.ServedGroups(); got.Len() != 0 {
		t.Errorf("served %v after every workspace was removed, want nothing", sets.List(got))
	}
}
