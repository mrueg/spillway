package apiserver

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"

	"github.com/mrueg/spillway/pkg/kcp"
)

// workspace is one kcp workspace and everything spillway needs to serve the
// groups it backs: what it currently offers, where to send requests, and the
// side channel admission and OpenAPI read.
type workspace struct {
	name   string
	config *rest.Config

	// fingerprint is the configuration this was built from, so a reload can
	// tell an unchanged workspace from one that has to be rebuilt.
	fingerprint string

	// cancel stops everything keeping this workspace current.
	cancel context.CancelFunc

	groups  *kcp.GroupMatcher
	cache   *kcp.ResourceCache
	proxy   *resourceProxy
	backend *backendClient
}

// router spreads spillway across the workspaces it was given.
//
// One workspace was an assumption baked into every component: the resource
// cache, the proxy, the OpenAPI documents and the APIService registrar each
// held the workspace they belonged to. They now hold this instead, and ask it
// per group -- which is the only question any of them actually had.
//
// A group belongs to the first workspace configured to serve it. Exact names
// are refused at startup if two workspaces claim one, so the order only decides
// between overlapping wildcards, where refusing would mean rejecting a
// perfectly reasonable "this group from here, everything else from there".
type router struct {
	// workspaces is replaced wholesale when the configuration is re-read, so
	// readers never see a half-applied set.
	workspaces atomic.Pointer[[]*workspace]

	// revision counts replacements. It is folded into the merged snapshot's key
	// because generations alone cannot detect a change to the set: removing a
	// workspace lowers their sum, and two different sets can sum alike.
	revision atomic.Uint64

	// merged caches the combined snapshot, which discovery reads on every
	// request. It is rebuilt when any workspace's generation moves.
	mu     sync.Mutex
	merged atomic.Pointer[mergedSnapshot]
}

type mergedSnapshot struct {
	generation uint64
	revision   uint64
	snapshot   *kcp.Snapshot
	owners     map[string]*workspace
}

func newRouter(workspaces []*workspace) *router {
	routes := &router{}
	routes.replace(workspaces)
	return routes
}

// replace swaps in a new set of workspaces.
func (r *router) replace(workspaces []*workspace) {
	set := make([]*workspace, len(workspaces))
	copy(set, workspaces)

	r.workspaces.Store(&set)
	r.revision.Add(1)
}

// snapshot returns the running workspaces.
func (r *router) snapshot() []*workspace {
	set := r.workspaces.Load()
	if set == nil {
		return nil
	}
	return *set
}

// Generation is the sum of the workspaces' generations: it moves whenever any
// of them does, which is all a cache key needs of it.
func (r *router) Generation() uint64 {
	var total uint64
	for _, workspace := range r.snapshot() {
		total += workspace.cache.Generation()
	}
	return total
}

// HasSynced reports whether every workspace has been read at least once.
// Anything less and the merged view is missing groups rather than describing a
// workspace that has none.
func (r *router) HasSynced() bool {
	for _, workspace := range r.snapshot() {
		if !workspace.cache.HasSynced() {
			return false
		}
	}
	return true
}

// Owns reports whether any workspace is configured to serve the group, whether
// or not it currently does.
func (r *router) Owns(group string) bool {
	return r.configuredFor(group) != nil
}

// configuredFor returns the workspace that would serve a group, from the
// configuration rather than from what is currently served.
func (r *router) configuredFor(group string) *workspace {
	for _, workspace := range r.snapshot() {
		if workspace.groups.Matches(group) {
			return workspace
		}
	}
	return nil
}

// Snapshot is every workspace's view of its own groups, as one.
func (r *router) Snapshot() *kcp.Snapshot {
	return r.current().snapshot
}

// ServedGroups is every group any workspace currently serves.
func (r *router) ServedGroups() sets.Set[string] {
	groups := sets.New[string]()
	for group := range r.current().snapshot.Groups {
		groups.Insert(group)
	}
	return groups
}

// servingFor returns the workspace a request for this group should go to: the
// one currently serving it, which for a group nobody serves is nothing.
func (r *router) servingFor(group string) *workspace {
	return r.current().owners[group]
}

// proxyFor is the handler for a group's resource requests.
func (r *router) proxyFor(group string) http.Handler {
	workspace := r.servingFor(group)
	if workspace == nil {
		return nil
	}
	return workspace.proxy
}

// fetcherFor is the side channel for a group's OpenAPI documents.
func (r *router) fetcherFor(group string) specFetcher {
	workspace := r.servingFor(group)
	if workspace == nil {
		return nil
	}
	return workspace.backend
}

// namedFetcher is a workspace's side channel and which workspace it is, so that
// one that cannot answer can be named rather than merely counted.
type namedFetcher struct {
	name    string
	fetcher specFetcher
}

// fetchers returns one side channel per workspace, for documents that describe
// the whole of what spillway serves rather than one group.
func (r *router) fetchers() []namedFetcher {
	workspaces := r.snapshot()
	fetchers := make([]namedFetcher, 0, len(workspaces))
	for _, workspace := range workspaces {
		fetchers = append(fetchers, namedFetcher{name: workspace.name, fetcher: workspace.backend})
	}
	return fetchers
}

// current returns the merged view, rebuilding it if any workspace has moved on.
func (r *router) current() *mergedSnapshot {
	generation, revision := r.Generation(), r.revision.Load()
	if merged := r.merged.Load(); merged != nil && merged.generation == generation && merged.revision == revision {
		return merged
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Another goroutine may have rebuilt it while this one waited.
	if merged := r.merged.Load(); merged != nil && merged.generation == generation && merged.revision == revision {
		return merged
	}

	merged := &mergedSnapshot{
		generation: generation,
		revision:   revision,
		snapshot: &kcp.Snapshot{
			Groups:    map[string]metav1.APIGroup{},
			Resources: map[schema.GroupVersion][]metav1.APIResource{},
		},
		owners: map[string]*workspace{},
	}

	for _, workspace := range r.snapshot() {
		snapshot := workspace.cache.Snapshot()
		for group, apiGroup := range snapshot.Groups {
			if _, taken := merged.snapshot.Groups[group]; taken {
				// Two workspaces serving one group: the first configured for it
				// wins, and the second's copy is not merged in. Exact names
				// cannot collide -- that is refused at startup -- so this is
				// overlapping wildcards, where silently interleaving two
				// workspaces' versions of a group would be worse than choosing.
				continue
			}
			merged.snapshot.Groups[group] = apiGroup
			merged.owners[group] = workspace
		}
		for gv, resources := range snapshot.Resources {
			if merged.owners[gv.Group] != workspace {
				continue
			}
			merged.snapshot.Resources[gv] = resources
		}
	}

	r.merged.Store(merged)
	return merged
}
