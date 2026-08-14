package kcp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

func TestMain(m *testing.M) {
	// Exercise the real metric objects rather than their uninitialized noop
	// forms, so a bad label or a nil metric fails here.
	RegisterMetrics()
	m.Run()
}

// matcherFor builds the group matcher the cache and the watch take, from the
// plain group names a test wants to talk about.
func matcherFor(t *testing.T, groups ...string) *GroupMatcher {
	t.Helper()

	matcher, err := ParseGroupMatcher(groups)
	if err != nil {
		t.Fatalf("parsing group matcher %v: %v", groups, err)
	}
	return matcher
}

// stubDiscovery implements only the two methods the cache uses. The embedded
// interface is nil, so any other call panics rather than silently passing.
type stubDiscovery struct {
	discovery.DiscoveryInterface

	groups    *metav1.APIGroupList
	resources map[string]*metav1.APIResourceList

	groupsErr    error
	resourcesErr error

	// Read from the test goroutine while Run's goroutine writes it.
	groupCalls atomic.Int64
}

func (s *stubDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	s.groupCalls.Add(1)
	if s.groupsErr != nil {
		return nil, s.groupsErr
	}
	return s.groups, nil
}

func (s *stubDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	if s.resourcesErr != nil {
		return nil, s.resourcesErr
	}
	list, found := s.resources[groupVersion]
	if !found {
		return nil, errors.New("no resources for " + groupVersion)
	}
	return list, nil
}

func widgetsDiscovery() *stubDiscovery {
	return &stubDiscovery{
		groups: &metav1.APIGroupList{Groups: []metav1.APIGroup{
			{
				Name:             "spillway.example.com",
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: "spillway.example.com/v1alpha1", Version: "v1alpha1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "spillway.example.com/v1alpha1", Version: "v1alpha1"},
			},
			// kcp's own APIs, which spillway must not pick up.
			{
				Name:     "tenancy.kcp.io",
				Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "tenancy.kcp.io/v1alpha1", Version: "v1alpha1"}},
			},
		}},
		resources: map[string]*metav1.APIResourceList{
			"spillway.example.com/v1alpha1": {APIResources: []metav1.APIResource{
				{Name: "widgets", Kind: "Widget", Namespaced: true},
			}},
			"tenancy.kcp.io/v1alpha1": {APIResources: []metav1.APIResource{
				{Name: "workspaces", Kind: "Workspace"},
			}},
		},
	}
}

func TestRefreshTracksOnlyOwnedGroups(t *testing.T) {
	cache := NewResourceCache("test", widgetsDiscovery(), matcherFor(t, "spillway.example.com"))

	if err := cache.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	snapshot := cache.Snapshot()
	if _, found := snapshot.Groups["spillway.example.com"]; !found {
		t.Error("the owned group is missing from the snapshot")
	}
	if _, found := snapshot.Groups["tenancy.kcp.io"]; found {
		t.Error("an unowned kcp group leaked into the snapshot")
	}

	gv := schema.GroupVersion{Group: "spillway.example.com", Version: "v1alpha1"}
	resources := snapshot.Resources[gv]
	if len(resources) != 1 || resources[0].Name != "widgets" {
		t.Errorf("Resources[%s] = %+v, want a single widgets entry", gv, resources)
	}
	if _, found := snapshot.Resources[schema.GroupVersion{Group: "tenancy.kcp.io", Version: "v1alpha1"}]; found {
		t.Error("resources for an unowned group leaked into the snapshot")
	}
}

func TestHasSyncedOnlyAfterSuccess(t *testing.T) {
	cache := NewResourceCache("test", widgetsDiscovery(), matcherFor(t, "spillway.example.com"))

	if cache.HasSynced() {
		t.Error("HasSynced is true before the first refresh")
	}
	if err := cache.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !cache.HasSynced() {
		t.Error("HasSynced is false after a successful refresh")
	}
}

func TestRefreshErrorBeforeFirstSync(t *testing.T) {
	client := widgetsDiscovery()
	client.groupsErr = errors.New("connection refused")
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	if err := cache.Refresh(); err == nil {
		t.Fatal("Refresh succeeded despite an unreachable kcp")
	}
	if cache.HasSynced() {
		t.Error("HasSynced is true even though no refresh ever succeeded")
	}
	if got := len(cache.Snapshot().Groups); got != 0 {
		t.Errorf("snapshot has %d groups, want 0", got)
	}
}

// The whole point of the cache is that a failing kcp does not empty discovery
// out from under the aggregation layer.
func TestRefreshKeepsPreviousSnapshotOnError(t *testing.T) {
	client := widgetsDiscovery()
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	if err := cache.Refresh(); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}

	client.groupsErr = errors.New("connection refused")
	if err := cache.Refresh(); err == nil {
		t.Fatal("second Refresh succeeded despite an unreachable kcp")
	}

	snapshot := cache.Snapshot()
	if _, found := snapshot.Groups["spillway.example.com"]; !found {
		t.Error("the previous snapshot was discarded when the refresh failed")
	}
	if !cache.HasSynced() {
		t.Error("HasSynced went false after a failed refresh")
	}
}

// A failure partway through must not leave a half-built snapshot in place.
func TestRefreshDiscardsPartialResults(t *testing.T) {
	client := widgetsDiscovery()
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	if err := cache.Refresh(); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}

	client.resourcesErr = errors.New("timeout listing resources")
	if err := cache.Refresh(); err == nil {
		t.Fatal("Refresh succeeded despite failing to list resources")
	}

	gv := schema.GroupVersion{Group: "spillway.example.com", Version: "v1alpha1"}
	if len(cache.Snapshot().Resources[gv]) != 1 {
		t.Error("the snapshot lost its resources when a partial refresh failed")
	}
}

func TestRunRefreshesUntilContextIsDone(t *testing.T) {
	client := widgetsDiscovery()
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	ctx, cancel := context.WithCancel(context.Background())
	changes := make(chan struct{}, 8)

	go cache.Run(ctx, time.Millisecond, nil, func() {
		select {
		case changes <- struct{}{}:
		default:
		}
	})

	select {
	case <-changes:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Run never reported a successful refresh")
	}
	cancel()

	if !cache.HasSynced() {
		t.Error("HasSynced is false after Run refreshed")
	}
}

// onChange must not fire for a refresh that failed, or callers would publish an
// unchanged snapshot as though it were fresh.
func TestRunSkipsOnChangeWhenRefreshFails(t *testing.T) {
	client := widgetsDiscovery()
	client.groupsErr = errors.New("connection refused")
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var changed atomic.Bool
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cache.Run(ctx, time.Millisecond, nil, func() { changed.Store(true) })
	}()

	// Waiting for the attempt rather than giving it a fixed budget: on a loaded
	// machine a goroutine can take longer to be scheduled than any deadline
	// short enough to keep this test quick, and "Run never called kcp" would
	// then mean "the machine was busy".
	deadline := time.After(15 * time.Second)
	for client.groupCalls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("Run never called kcp at all")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	<-stopped

	if changed.Load() {
		t.Error("onChange fired even though every refresh failed")
	}
}

// A change signal is what makes a new CRD show up promptly instead of waiting
// out the backstop.
func TestRunRefreshesOnAChangeSignal(t *testing.T) {
	client := widgetsDiscovery()
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changed := make(chan struct{}, 1)
	refreshed := make(chan struct{}, 8)

	// A backstop long enough that reaching it would mean the signal did nothing.
	go cache.Run(ctx, time.Hour, changed, func() {
		select {
		case refreshed <- struct{}{}:
		default:
		}
	})

	changed <- struct{}{}

	select {
	case <-refreshed:
	case <-time.After(30 * time.Second):
		t.Fatal("a change signal did not produce a refresh")
	}

	if !cache.HasSynced() {
		t.Error("HasSynced is false after a signalled refresh")
	}
}

// Bursts are the normal case -- applying a bundle of CRDs -- and each object
// must not cost its own re-read of discovery.
func TestRunCoalescesABurstOfChanges(t *testing.T) {
	client := widgetsDiscovery()
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changed := make(chan struct{}, 1)
	go cache.Run(ctx, time.Hour, changed, nil)

	// The channel holds one pending signal, so the rest of the burst is dropped
	// rather than queued.
	for range 20 {
		select {
		case changed <- struct{}{}:
		default:
		}
	}

	deadline := time.After(30 * time.Second)
	for cache.Snapshot() == nil || !cache.HasSynced() {
		select {
		case <-deadline:
			t.Fatal("the burst never produced a refresh")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Far fewer calls than signals: one for the burst, and at most one more for
	// a signal that arrived while the first refresh was running.
	if calls := client.groupCalls.Load(); calls > 3 {
		t.Errorf("kcp was queried %d times for a burst of 20 signals; the burst is not being coalesced", calls)
	}
}

// Without a watch the loop still works, it is just polling.
func TestRunWithoutAChangeChannel(t *testing.T) {
	client := widgetsDiscovery()
	cache := NewResourceCache("test", client, matcherFor(t, "spillway.example.com"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	refreshed := make(chan struct{}, 1)
	go cache.Run(ctx, 10*time.Millisecond, nil, func() {
		select {
		case refreshed <- struct{}{}:
		default:
		}
	})

	select {
	case <-refreshed:
	case <-time.After(30 * time.Second):
		t.Fatal("the backstop never fired")
	}
}

// time.NewTicker panics on a non-positive interval, which would take the
// process down from inside a goroutine rather than failing a flag check.
func TestRunSurvivesANonPositiveBackstop(t *testing.T) {
	cache := NewResourceCache("test", widgetsDiscovery(), matcherFor(t, "spillway.example.com"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changed := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.Run(ctx, 0, changed, nil)
	}()

	changed <- struct{}{}
	deadline := time.After(30 * time.Second)
	for !cache.HasSynced() {
		select {
		case <-deadline:
			t.Fatal("Run never refreshed with a zero backstop")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}
