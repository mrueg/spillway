package apiserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type recordingNamespaces struct {
	existing map[string]bool
	fetched  []string
	created  []string

	createErr error
}

func (r *recordingNamespaces) fetch(_ context.Context, path string) ([]byte, error) {
	r.fetched = append(r.fetched, path)
	if r.existing[path] {
		return []byte(`{"kind":"Namespace"}`), nil
	}
	return nil, &backendStatusError{status: http.StatusNotFound, path: path}
}

func (r *recordingNamespaces) create(_ context.Context, path string, body []byte) ([]byte, error) {
	r.created = append(r.created, string(body))
	if r.createErr != nil {
		return nil, r.createErr
	}
	r.existing[path] = true
	return body, nil
}

func TestNamespaceMirrorCreatesWhatIsMissing(t *testing.T) {
	backend := &recordingNamespaces{existing: map[string]bool{}}
	mirror := newNamespaceMirror(backend)

	mirror.ensure(context.Background(), "demo")

	if len(backend.created) != 1 {
		t.Fatalf("created %v, want one namespace", backend.created)
	}
	for _, want := range []string{`"name":"demo"`, managedByLabel} {
		if !strings.Contains(backend.created[0], want) {
			t.Errorf("created namespace %s, want it to contain %s", backend.created[0], want)
		}
	}
}

// The check costs a round trip, so it must happen once per namespace rather
// than once per write.
func TestNamespaceMirrorAsksOnce(t *testing.T) {
	backend := &recordingNamespaces{existing: map[string]bool{namespacePath + "/demo": true}}
	mirror := newNamespaceMirror(backend)

	for range 5 {
		mirror.ensure(context.Background(), "demo")
	}

	if len(backend.fetched) != 1 {
		t.Errorf("asked kcp %d times for the same namespace, want once", len(backend.fetched))
	}
	if len(backend.created) != 0 {
		t.Errorf("created %v for a namespace that already exists", backend.created)
	}
}

// Two replicas will race for the same namespace. Losing that race is both of
// them getting what they wanted.
func TestNamespaceMirrorAcceptsLosingTheRace(t *testing.T) {
	backend := &recordingNamespaces{
		existing:  map[string]bool{},
		createErr: &backendStatusError{status: http.StatusConflict, path: namespacePath},
	}
	mirror := newNamespaceMirror(backend)

	mirror.ensure(context.Background(), "demo")
	mirror.ensure(context.Background(), "demo")

	if len(backend.created) != 1 {
		t.Errorf("attempted %d creates, want one: a conflict means it is already there", len(backend.created))
	}
}

// A mirror that cannot create must not swallow the write. kcp's own rejection
// names the namespace and the object; spillway has nothing better to say.
func TestNamespaceMirrorGivesUpQuietly(t *testing.T) {
	backend := &recordingNamespaces{existing: map[string]bool{}, createErr: errors.New("kcp unreachable")}
	mirror := newNamespaceMirror(backend)

	mirror.ensure(context.Background(), "demo")

	// Not remembered, so the next write tries again rather than assuming it is
	// there.
	mirror.ensure(context.Background(), "demo")
	if len(backend.created) != 2 {
		t.Errorf("attempted %d creates, want it to retry after a failure", len(backend.created))
	}
}

// Only a write brings a namespace into being. A read of one that is not there
// is empty rather than wrong, and a delete has nothing to create for.
func TestMirrorsNamespaceOnlyForWrites(t *testing.T) {
	for method, want := range map[string]bool{
		http.MethodPost:   true,
		http.MethodPut:    true,
		http.MethodPatch:  true,
		http.MethodGet:    false,
		http.MethodDelete: false,
	} {
		req := httptest.NewRequest(method, "/apis/g/v1/namespaces/demo/widgets", nil)
		if got := mirrorsNamespace(req); got != want {
			t.Errorf("mirrorsNamespace(%s) = %v, want %v", method, got, want)
		}
	}
}

// The cache is what keeps the check to one round trip, but believing in a
// namespace forever means a namespace deleted in the workspace fails every
// write into it until spillway restarts.
func TestNamespaceMirrorForgetsWhatItSaw(t *testing.T) {
	backend := &recordingNamespaces{existing: map[string]bool{namespacePath + "/demo": true}}
	mirror := newNamespaceMirror(backend)

	now := time.Now()
	mirror.now = func() time.Time { return now }

	mirror.ensure(context.Background(), "demo")
	mirror.ensure(context.Background(), "demo")
	if len(backend.fetched) != 1 {
		t.Fatalf("asked %d times inside the TTL, want once", len(backend.fetched))
	}

	// The namespace is removed from the workspace, and the TTL runs out.
	delete(backend.existing, namespacePath+"/demo")
	now = now.Add(namespaceTTL + time.Second)

	mirror.ensure(context.Background(), "demo")
	if len(backend.fetched) != 2 {
		t.Errorf("asked %d times after the TTL, want it to check again", len(backend.fetched))
	}
	if len(backend.created) != 1 {
		t.Errorf("created %v, want the namespace recreated once it was gone", backend.created)
	}
}

// A cluster-scoped resource has no namespace to mirror, and asking kcp about
// one would be a round trip to look up the empty string.
func TestNamespaceMirrorIgnoresClusterScopedWrites(t *testing.T) {
	backend := &recordingNamespaces{existing: map[string]bool{}}
	mirror := newNamespaceMirror(backend)

	mirror.ensure(context.Background(), "")

	if len(backend.fetched) != 0 || len(backend.created) != 0 {
		t.Errorf("asked kcp about the empty namespace: fetched=%v created=%v", backend.fetched, backend.created)
	}
}
