package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// namespacePath is where a workspace keeps its namespaces.
const namespacePath = "/api/v1/namespaces"

// namespaceMirror creates a namespace in the workspace the first time something
// is written into it.
//
// A namespace has to exist in both places: the cluster's, because spillway runs
// that cluster's namespace admission and refuses a write into a namespace it
// does not have, and the workspace's, because that is where the object is
// stored. Creating both by hand is the step everyone forgets, and the error
// when they do -- kcp reporting a namespace it has never heard of -- reads like
// the bridge is broken rather than like a missing namespace.
//
// It mirrors on demand rather than reconciling every namespace the cluster has.
// A workspace is not a copy of a cluster, and filling it with kube-system,
// kube-public and every namespace of an unrelated tenant to make one widget
// writable is the wrong trade. What gets created is what gets used.
//
// It never deletes. Deleting a cluster namespace already removes the offloaded
// objects in it -- the cluster's own namespace controller reaches them through
// spillway -- and removing the workspace's namespace as well would delete
// whatever else the workspace kept there, which spillway did not put there and
// cannot account for.
type namespaceMirror struct {
	backend namespaceBackend

	// ttl bounds how long a namespace is assumed to still be there. The cache
	// exists so the check costs one round trip rather than one per write, but a
	// namespace deleted in the workspace afterwards would otherwise be believed
	// in until spillway restarted, and every write into it would fail with an
	// error the operator has no way to clear.
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	known map[string]time.Time
}

// namespaceTTL is how long the mirror trusts what it last saw. Long enough that
// a busy namespace costs nothing, short enough that recreating one in the
// workspace takes effect without a restart.
const namespaceTTL = 10 * time.Minute

// namespaceBackend is the part of the kcp client this needs.
type namespaceBackend interface {
	fetch(ctx context.Context, path string) ([]byte, error)
	create(ctx context.Context, path string, body []byte) ([]byte, error)
}

func newNamespaceMirror(backend namespaceBackend) *namespaceMirror {
	return &namespaceMirror{
		backend: backend,
		ttl:     namespaceTTL,
		now:     time.Now,
		known:   map[string]time.Time{},
	}
}

// ensure makes sure the namespace exists in the workspace.
//
// It reports no error to the caller by design. If the namespace cannot be
// created, the write is forwarded anyway and kcp answers for itself: its
// rejection names the namespace and the object, which is a better error than
// anything spillway could substitute for it.
func (m *namespaceMirror) ensure(ctx context.Context, namespace string) {
	if m == nil || namespace == "" {
		return
	}

	if m.fresh(namespace) {
		return
	}

	log := klog.FromContext(ctx)

	// A read first, so the usual case -- the namespace is already there --
	// costs one round trip once, and nothing afterwards.
	if _, err := m.backend.fetch(ctx, namespacePath+"/"+namespace); err == nil {
		m.remember(namespace)
		return
	}

	body, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": namespace,
			// Marked so an operator can tell what spillway put there from what
			// the workspace already had, which is what makes cleaning up
			// possible at all given nothing here deletes.
			"labels": map[string]string{managedByLabel: managedByValue},
		},
	})
	if err != nil {
		log.Error(err, "Encoding a namespace for the workspace", "namespace", namespace)
		return
	}

	if _, err := m.backend.create(ctx, namespacePath, body); err != nil {
		// Another replica may have created it in the meantime, which is not a
		// failure: both of them wanted the same thing.
		if isAlreadyExists(err) {
			m.remember(namespace)
			return
		}
		log.V(2).Info("Could not mirror a namespace into the workspace; forwarding the write anyway",
			"namespace", namespace, "err", err)
		return
	}

	log.V(2).Info("Created a namespace in the workspace", "namespace", namespace)
	m.remember(namespace)
}

// fresh reports whether the namespace was confirmed recently enough to skip
// asking again.
func (m *namespaceMirror) fresh(namespace string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen, found := m.known[namespace]
	if !found {
		return false
	}
	if m.now().Sub(seen) >= m.ttl {
		// Forgotten rather than left to grow: a cluster that churns through
		// namespaces would otherwise accumulate one entry per namespace that
		// ever existed.
		delete(m.known, namespace)
		return false
	}
	return true
}

func (m *namespaceMirror) remember(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.known[namespace] = m.now()
}

// isAlreadyExists reads kcp's own answer rather than a Go error string.
func isAlreadyExists(err error) bool {
	status := &backendStatusError{}
	if !errors.As(err, &status) {
		return false
	}
	return status.status == http.StatusConflict
}

// mirrorsNamespace reports whether a request is one that should bring its
// namespace into being: a write of an object into it. A read of a namespace
// that does not exist is empty rather than wrong, and a delete has nothing to
// create for.
func mirrorsNamespace(req *http.Request) bool {
	switch req.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	default:
		return false
	}
}
