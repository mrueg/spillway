package apiserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest"
)

func testBackend() BackendOptions {
	return BackendOptions{
		RequestTimeout:   30 * time.Second,
		Retries:          2,
		FailureThreshold: 5,
		CircuitCooldown:  10 * time.Second,
	}
}

// servedGroups is the dynamic answer the proxy asks for, from a fixed list.
func servedGroups(groups ...string) func() sets.Set[string] {
	return func() sets.Set[string] { return sets.New(groups...) }
}

func TestNewResourceProxyRejectsRelativeHost(t *testing.T) {
	for _, host := range []string{"", "not-a-url", "/clusters/root:spillway"} {
		if _, err := newResourceProxy("test", "", &rest.Config{Host: host}, false, testBackend(), servedGroups("spillway.example.com")); err == nil {
			t.Errorf("newResourceProxy(%q) succeeded, want an error about an absolute URL", host)
		}
	}
}

// The workspace is selected by a path prefix on the kcp server URL, so every
// proxied request has to carry it. Getting this wrong sends reads to the root
// workspace, which would quietly return the wrong objects rather than fail.
func TestProxyTargetPrependsWorkspacePath(t *testing.T) {
	proxy, err := newResourceProxy("test", "", &rest.Config{Host: "https://kcp.example:6443/clusters/root:spillway"}, false, testBackend(), servedGroups("spillway.example.com"))
	if err != nil {
		t.Fatalf("newResourceProxy: %v", err)
	}

	for _, tc := range []struct {
		name     string
		request  string
		wantPath string
		wantRaw  string
	}{
		{
			name:     "list",
			request:  "/apis/spillway.example.com/v1alpha1/namespaces/default/widgets",
			wantPath: "/clusters/root:spillway/apis/spillway.example.com/v1alpha1/namespaces/default/widgets",
		},
		{
			name:     "get",
			request:  "/apis/spillway.example.com/v1alpha1/namespaces/default/widgets/red-widget",
			wantPath: "/clusters/root:spillway/apis/spillway.example.com/v1alpha1/namespaces/default/widgets/red-widget",
		},
		{
			name:     "watch preserves the query",
			request:  "/apis/spillway.example.com/v1alpha1/widgets?watch=true&resourceVersion=42",
			wantPath: "/clusters/root:spillway/apis/spillway.example.com/v1alpha1/widgets",
			wantRaw:  "watch=true&resourceVersion=42",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := proxy.targetFor(httptest.NewRequest(http.MethodGet, tc.request, nil))

			if target.Path != tc.wantPath {
				t.Errorf("Path = %q, want %q", target.Path, tc.wantPath)
			}
			if target.RawQuery != tc.wantRaw {
				t.Errorf("RawQuery = %q, want %q", target.RawQuery, tc.wantRaw)
			}
			if target.Host != "kcp.example:6443" {
				t.Errorf("Host = %q, want kcp.example:6443", target.Host)
			}
			if target.Scheme != "https" {
				t.Errorf("Scheme = %q, want https", target.Scheme)
			}
		})
	}
}

// A workspace served at the root of a shard has no prefix to prepend.
func TestProxyTargetWithoutWorkspacePath(t *testing.T) {
	proxy, err := newResourceProxy("test", "", &rest.Config{Host: "https://kcp.example:6443"}, false, testBackend(), servedGroups("spillway.example.com"))
	if err != nil {
		t.Fatalf("newResourceProxy: %v", err)
	}

	target := proxy.targetFor(httptest.NewRequest(http.MethodGet, "/apis/spillway.example.com/v1alpha1/widgets", nil))
	if want := "/apis/spillway.example.com/v1alpha1/widgets"; target.Path != want {
		t.Errorf("Path = %q, want %q", target.Path, want)
	}
}

// recordingTransport captures the request that would have gone to kcp.
type recordingTransport struct{ got *http.Request }

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.got = req.Clone(req.Context())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader("{}")),
		Request:    req,
	}, nil
}

func proxyRequestAs(t *testing.T, impersonate bool, caller user.Info) *http.Request {
	t.Helper()

	location, err := url.Parse("https://kcp.example:6443/clusters/root:spillway")
	if err != nil {
		t.Fatalf("parsing the location: %v", err)
	}
	transport := &recordingTransport{}
	proxy := &resourceProxy{location: location, transport: transport, impersonate: impersonate}

	req := httptest.NewRequest(http.MethodGet, "/apis/spillway.example.com/v1alpha1/widgets", nil)
	// The token the caller presented to the workload cluster.
	req.Header.Set("Authorization", "Bearer caller-token")
	if caller != nil {
		req = req.WithContext(genericapirequest.WithUser(req.Context(), caller))
	}

	proxy.ServeHTTP(httptest.NewRecorder(), req)

	if transport.got == nil {
		t.Fatal("the proxy never issued a request to kcp")
	}
	return transport.got
}

// The caller's bearer token is for the workload cluster and means nothing to
// kcp. Worse, client-go only attaches spillway's own credentials when
// Authorization is unset, so leaving it would send the request unauthenticated.
func TestProxyStripsTheCallersCredentials(t *testing.T) {
	for _, impersonate := range []bool{false, true} {
		got := proxyRequestAs(t, impersonate, &user.DefaultInfo{Name: "alice"})

		if authorization := got.Header.Get("Authorization"); authorization != "" {
			t.Errorf("impersonate=%v: Authorization forwarded to kcp as %q, want it stripped",
				impersonate, authorization)
		}
	}
}

func TestProxyImpersonatesTheCaller(t *testing.T) {
	got := proxyRequestAs(t, true, &user.DefaultInfo{
		Name:   "alice",
		UID:    "uid-1",
		Groups: []string{"developers", "system:authenticated"},
		Extra:  map[string][]string{"scopes": {"read"}},
	})

	if name := got.Header.Get("Impersonate-User"); name != "alice" {
		t.Errorf("Impersonate-User = %q, want alice", name)
	}
	if uid := got.Header.Get("Impersonate-Uid"); uid != "uid-1" {
		t.Errorf("Impersonate-Uid = %q, want uid-1", uid)
	}
	groups := got.Header.Values("Impersonate-Group")
	if len(groups) != 2 || groups[0] != "developers" {
		t.Errorf("Impersonate-Group = %v, want the caller's groups", groups)
	}
	if scopes := got.Header.Values("Impersonate-Extra-Scopes"); len(scopes) != 1 || scopes[0] != "read" {
		t.Errorf("Impersonate-Extra-Scopes = %v, want [read]", scopes)
	}
}

// The default is that requests reach kcp as spillway itself, after the workload
// cluster's RBAC has already authorized them.
func TestProxyDoesNotImpersonateByDefault(t *testing.T) {
	got := proxyRequestAs(t, false, &user.DefaultInfo{Name: "alice"})

	for _, header := range []string{"Impersonate-User", "Impersonate-Uid", "Impersonate-Group"} {
		if value := got.Header.Get(header); value != "" {
			t.Errorf("%s = %q with impersonation off, want it unset", header, value)
		}
	}
}

// Impersonating nobody would silently act as spillway, so a request that
// somehow arrives unauthenticated must not reach kcp at all.
func TestProxyRefusesToImpersonateWithoutAUser(t *testing.T) {
	location, err := url.Parse("https://kcp.example:6443/clusters/root:spillway")
	if err != nil {
		t.Fatalf("parsing the location: %v", err)
	}
	transport := &recordingTransport{}
	proxy := &resourceProxy{location: location, transport: transport, impersonate: true}

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/apis/spillway.example.com/v1alpha1/widgets", nil))

	if transport.got != nil {
		t.Error("an unauthenticated request was forwarded to kcp")
	}
	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", recorder.Code)
	}
}

// The mux dispatches by raw prefix match, so a path that resolves elsewhere
// still arrives at this handler. Joining it onto the workspace prefix would
// address another workspace, or escape the prefix entirely, with spillway's
// credentials -- while the authorization that admitted the request was decided
// from the path as written.
func TestProxyRefusesNonCanonicalPaths(t *testing.T) {
	transport := &recordingTransport{}
	location, err := url.Parse("https://kcp.example:6443/clusters/root:spillway")
	if err != nil {
		t.Fatalf("parsing the location: %v", err)
	}
	proxy := &resourceProxy{location: location, transport: transport}

	for _, escape := range []string{
		"/apis/spillway.example.com/v1alpha1/../../../../clusters/root:other/apis/g/v1/secrets",
		"/apis/spillway.example.com/v1alpha1/widgets/../../../../../healthz",
		"/apis/spillway.example.com/v1alpha1/./widgets",
		"//apis/spillway.example.com/v1alpha1/widgets",
	} {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://spillway"+escape, nil)
		req.URL.Path = escape // keep it exactly as sent

		proxy.ServeHTTP(recorder, req)

		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", escape, recorder.Code)
		}
		if transport.got != nil {
			t.Fatalf("%s: was forwarded to kcp as %q", escape, transport.got.URL.Path)
		}
	}
}

func TestProxyAcceptsOrdinaryPaths(t *testing.T) {
	for _, ordinary := range []string{
		"/apis/spillway.example.com/v1alpha1/widgets",
		"/apis/spillway.example.com/v1alpha1/namespaces/default/widgets/red-widget",
		"/apis/spillway.example.com/v1alpha1/namespaces/default/widgets/red-widget/status",
		"/apis/spillway.example.com/v1alpha1/widgets/", // a trailing slash is not traversal
	} {
		if !canonicalPath(ordinary) {
			t.Errorf("%s was rejected as non-canonical", ordinary)
		}
	}
}
