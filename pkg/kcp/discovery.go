package kcp

import (
	"context"
	"fmt"
	"time"

	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/klog/v2"
)

// Snapshot is an immutable view of the API surface a kcp workspace offers for
// the groups spillway owns.
type Snapshot struct {
	// Groups is keyed by group name and holds discovery as kcp reported it,
	// including which version kcp prefers.
	Groups map[string]metav1.APIGroup
	// Resources is keyed by group version.
	Resources map[schema.GroupVersion][]metav1.APIResource
}

// ResourceCache tracks the API resources a kcp workspace serves for a fixed set
// of groups.
//
// Discovery is answered from this cache rather than from kcp directly. The
// aggregation layer's availability probe has a six second budget and flips the
// APIService -- degrading discovery for the whole cluster -- when it is missed,
// so a slow or briefly unreachable kcp must not be on that path.
type ResourceCache struct {
	client discovery.DiscoveryInterface
	owned  *GroupMatcher

	// workspace names this cache in the metrics. One workspace being current
	// says nothing about another, so every series it writes is labelled.
	workspace string

	snapshot atomic.Pointer[Snapshot]
	synced   atomic.Bool

	// matched is the set of groups the previous refresh saw, so that a group
	// which disappears can have its metric series removed.
	matched atomic.Pointer[sets.Set[string]]

	// generation advances on every successful refresh. Anything derived from
	// the snapshot -- the OpenAPI documents above all -- uses it to tell
	// whether what it holds was built from the current API surface.
	generation atomic.Uint64
}

// NewResourceCache returns a cache that tracks the groups the matcher covers.
// It is empty until Refresh succeeds for the first time.
func NewResourceCache(workspace string, client discovery.DiscoveryInterface, owned *GroupMatcher) *ResourceCache {
	cache := &ResourceCache{
		workspace: workspace,
		client:    client,
		owned:     owned,
	}
	cache.snapshot.Store(&Snapshot{
		Groups:    map[string]metav1.APIGroup{},
		Resources: map[schema.GroupVersion][]metav1.APIResource{},
	})
	return cache
}

// ServedGroups returns the groups the workspace currently serves for spillway.
// It is the dynamic answer to "which groups are mine", which a pattern makes a
// question that only the latest snapshot can answer.
func (c *ResourceCache) ServedGroups() sets.Set[string] {
	return groupNames(c.Snapshot())
}

// Owns reports whether a group is one spillway is configured to serve, whether
// or not the workspace serves it right now. Withdrawing a registration for a
// group that has just disappeared needs this rather than ServedGroups.
func (c *ResourceCache) Owns(group string) bool {
	return c.owned.Matches(group)
}

func groupNames(snapshot *Snapshot) sets.Set[string] {
	names := sets.New[string]()
	for group := range snapshot.Groups {
		names.Insert(group)
	}
	return names
}

// Snapshot returns the most recent successful view. It is never nil.
func (c *ResourceCache) Snapshot() *Snapshot {
	return c.snapshot.Load()
}

// Generation identifies the API surface currently held. It advances on every
// successful refresh, so a consumer can cache work derived from the snapshot
// and notice when that work is out of date.
func (c *ResourceCache) Generation() uint64 {
	return c.generation.Load()
}

// HasSynced reports whether the cache has been populated at least once.
func (c *ResourceCache) HasSynced() bool {
	return c.synced.Load()
}

// Refresh replaces the snapshot with the current state of the workspace. On
// error the previous snapshot is left in place: serving slightly stale
// discovery beats losing availability over a transient failure.
func (c *ResourceCache) Refresh() error {
	start := time.Now()
	err := c.refresh()
	refreshDuration.WithLabelValues(c.workspace).Observe(time.Since(start).Seconds())

	if err != nil {
		refreshTotal.WithLabelValues(c.workspace, "error").Inc()
		return err
	}

	refreshTotal.WithLabelValues(c.workspace, "success").Inc()
	lastSuccessTimestamp.WithLabelValues(c.workspace).SetToCurrentTime()

	snapshot := c.Snapshot()

	// A group named outright reports 0 when the workspace does not serve it,
	// because somebody said it should be there. A group that arrived through a
	// pattern has no such expectation, so its series only exists while it does.
	for _, group := range c.owned.Exact() {
		served := 0.0
		if _, found := snapshot.Groups[group]; found {
			served = 1
		}
		groupServed.WithLabelValues(group).Set(served)
	}
	current := groupNames(snapshot)
	for group := range current {
		groupServed.WithLabelValues(group).Set(1)
	}
	if previous := c.matched.Swap(&current); previous != nil {
		for _, group := range previous.Difference(current).UnsortedList() {
			// Gone from the workspace, and never named outright: drop the series
			// rather than leave a gauge that will never move again.
			if !c.owned.exact.Has(group) {
				groupServed.DeleteLabelValues(group)
			}
		}
	}

	return nil
}

func (c *ResourceCache) refresh() error {
	groupList, err := c.client.ServerGroups()
	if err != nil {
		return fmt.Errorf("listing API groups in kcp: %w", err)
	}

	next := &Snapshot{
		Groups:    map[string]metav1.APIGroup{},
		Resources: map[schema.GroupVersion][]metav1.APIResource{},
	}

	for _, group := range groupList.Groups {
		if !c.owned.Matches(group.Name) {
			continue
		}
		next.Groups[group.Name] = group

		for _, version := range group.Versions {
			gv, err := schema.ParseGroupVersion(version.GroupVersion)
			if err != nil {
				return fmt.Errorf("parsing group version %q reported by kcp: %w", version.GroupVersion, err)
			}
			list, err := c.client.ServerResourcesForGroupVersion(version.GroupVersion)
			if err != nil {
				return fmt.Errorf("listing resources for %s in kcp: %w", version.GroupVersion, err)
			}
			next.Resources[gv] = list.APIResources
		}
	}

	c.snapshot.Store(next)
	c.synced.Store(true)
	c.generation.Add(1)
	return nil
}

// defaultBackstop is used when a caller asks for no periodic refresh at all.
// Run must not be left with only a watch: a watch cannot report what it never
// saw, and time.NewTicker panics on a non-positive interval.
const defaultBackstop = 10 * time.Minute

// settle is how long a change signal waits for company. Applying a bundle of
// CRDs produces a burst, and one refresh covers all of them.
const settle = 500 * time.Millisecond

// Run keeps the cache current until ctx is done, calling onChange after each
// successful refresh.
//
// Refreshes are driven by changed, so a new CRD in the workspace shows up in
// about as long as it takes kcp to establish it. backstop is the interval for
// refreshing anyway: a watch cannot report what it never saw, and the API
// surface can also move for reasons no CRD event covers.
//
// A nil changed channel leaves only the backstop, which is to say polling.
func (c *ResourceCache) Run(ctx context.Context, backstop time.Duration, changed <-chan struct{}, onChange func()) {
	if backstop <= 0 {
		backstop = defaultBackstop
	}

	ticker := time.NewTicker(backstop)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-changed:
			// Let the rest of the burst arrive before re-reading discovery.
			timer := time.NewTimer(settle)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		if err := c.Refresh(); err != nil {
			klog.FromContext(ctx).Error(err, "Refreshing kcp discovery; continuing to serve the previous snapshot")
			continue
		}
		if onChange != nil {
			onChange()
		}
	}
}
