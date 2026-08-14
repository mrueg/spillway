package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kube-openapi/pkg/handler3"

	"github.com/mrueg/spillway/pkg/kcp"
)

const testGroup = "spillway.example.com"

type fakeSnapshotter struct{ snapshot *kcp.Snapshot }

func (f fakeSnapshotter) Snapshot() *kcp.Snapshot { return f.snapshot }

// widgetSnapshot mirrors what the cache holds for a workspace serving a single
// namespaced CRD with a status subresource.
func widgetSnapshot() *kcp.Snapshot {
	gv := schema.GroupVersion{Group: testGroup, Version: "v1alpha1"}
	return &kcp.Snapshot{
		Groups: map[string]metav1.APIGroup{
			testGroup: {
				Name:             testGroup,
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: gv.String(), Version: gv.Version}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: gv.String(), Version: gv.Version},
			},
		},
		Resources: map[schema.GroupVersion][]metav1.APIResource{
			gv: {
				{Name: "widgets", SingularName: "widget", Namespaced: true, Kind: "Widget", Verbs: metav1.Verbs{"get", "list"}},
				{Name: "widgets/status", Namespaced: true, Kind: "Widget", Verbs: metav1.Verbs{"get"}},
			},
		},
	}
}

// staticProxy is one handler for every group, which is what a test with a
// single workspace wants.
func staticProxy(handler http.Handler) func(string) http.Handler {
	return func(string) http.Handler { return handler }
}

func newDiscoveryHandler() *discoveryHandler {
	return &discoveryHandler{
		cache: fakeSnapshotter{snapshot: widgetSnapshot()},
		owns:  func(group string) bool { return group == testGroup },
	}
}

func get(t *testing.T, handler func(http.ResponseWriter, *http.Request), path string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Accept", "application/json")
	handler(recorder, req)
	return recorder
}

// This is the exact path kube-aggregator probes to decide whether the
// APIService is Available, so it has to answer 200 with a resource list.
func TestServeVersionDiscovery(t *testing.T) {
	h := newDiscoveryHandler()
	recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
		h.serveUnderGroup(testGroup, w, r)
	}, "/apis/"+testGroup+"/v1alpha1")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body)
	}

	var list metav1.APIResourceList
	if err := json.Unmarshal(recorder.Body.Bytes(), &list); err != nil {
		t.Fatalf("decoding the response: %v; body: %s", err, recorder.Body)
	}
	if list.Kind != "APIResourceList" {
		t.Errorf("Kind = %q, want APIResourceList", list.Kind)
	}
	if list.GroupVersion != testGroup+"/v1alpha1" {
		t.Errorf("GroupVersion = %q, want %s/v1alpha1", list.GroupVersion, testGroup)
	}
	if len(list.APIResources) != 2 {
		t.Errorf("got %d resources, want 2 (widgets and widgets/status)", len(list.APIResources))
	}
}

func TestServeGroupDiscovery(t *testing.T) {
	h := newDiscoveryHandler()
	recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
		h.serveGroup(testGroup, w, r)
	}, "/apis/"+testGroup)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body)
	}

	var group metav1.APIGroup
	if err := json.Unmarshal(recorder.Body.Bytes(), &group); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}
	if group.Name != testGroup {
		t.Errorf("Name = %q, want %q", group.Name, testGroup)
	}
	if group.PreferredVersion.Version != "v1alpha1" {
		t.Errorf("PreferredVersion = %q, want v1alpha1", group.PreferredVersion.Version)
	}
}

func TestServeUnknownVersionIsNotFound(t *testing.T) {
	h := newDiscoveryHandler()
	recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
		h.serveUnderGroup(testGroup, w, r)
	}, "/apis/"+testGroup+"/v99")

	if recorder.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404; body: %s", recorder.Code, recorder.Body)
	}
}

// A group the workspace has stopped serving must not keep answering from a
// stale registration.
func TestServeGroupMissingFromSnapshot(t *testing.T) {
	h := &discoveryHandler{
		cache: fakeSnapshotter{snapshot: &kcp.Snapshot{Groups: map[string]metav1.APIGroup{}}},
		owns:  func(group string) bool { return group == testGroup },
	}
	recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
		h.serveGroup(testGroup, w, r)
	}, "/apis/"+testGroup)

	if recorder.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404; body: %s", recorder.Code, recorder.Body)
	}
}

func TestResourceRequestsAreProxied(t *testing.T) {
	var proxied []string
	h := newDiscoveryHandler()
	h.proxy = staticProxy(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied = append(proxied, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))

	paths := []string{
		"/apis/" + testGroup + "/v1alpha1/widgets",
		"/apis/" + testGroup + "/v1alpha1/namespaces/default/widgets",
		"/apis/" + testGroup + "/v1alpha1/namespaces/default/widgets/red-widget/status",
	}
	for _, path := range paths {
		recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
			h.serveUnderGroup(testGroup, w, r)
		}, path)
		if recorder.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", path, recorder.Code)
		}
	}

	if len(proxied) != len(paths) {
		t.Fatalf("proxied %v, want all of %v", proxied, paths)
	}
}

// Discovery must keep being answered locally: it comes from the cache so that a
// slow kcp cannot delay the aggregation layer's availability probe.
func TestDiscoveryIsNotProxied(t *testing.T) {
	h := newDiscoveryHandler()
	h.proxy = staticProxy(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("discovery request for %s was proxied to kcp", r.URL.Path)
	}))

	recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
		h.serveUnderGroup(testGroup, w, r)
	}, "/apis/"+testGroup+"/v1alpha1")

	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", recorder.Code)
	}
}

// An unknown version must not reach kcp at all.
func TestUnknownVersionIsNotProxied(t *testing.T) {
	h := newDiscoveryHandler()
	h.proxy = staticProxy(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("request for an unserved version was proxied to kcp: %s", r.URL.Path)
	}))

	recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
		h.serveUnderGroup(testGroup, w, r)
	}, "/apis/"+testGroup+"/v99/widgets")

	if recorder.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", recorder.Code)
	}
}

func TestResourceRequestsAreNotImplemented(t *testing.T) {
	h := newDiscoveryHandler()

	for _, path := range []string{
		"/apis/" + testGroup + "/v1alpha1/widgets",
		"/apis/" + testGroup + "/v1alpha1/namespaces/default/widgets",
		"/apis/" + testGroup + "/v1alpha1/namespaces/default/widgets/red-widget",
	} {
		recorder := get(t, func(w http.ResponseWriter, r *http.Request) {
			h.serveUnderGroup(testGroup, w, r)
		}, path)

		if recorder.Code != http.StatusNotImplemented {
			t.Errorf("%s: status = %d, want 501; body: %s", path, recorder.Code, recorder.Body)
			continue
		}
		// The message should name the resource asked for, not the "namespaces"
		// segment of the path.
		if !strings.Contains(recorder.Body.String(), "widgets") {
			t.Errorf("%s: response does not mention widgets: %s", path, recorder.Body)
		}
	}
}

type fakeFetcher struct {
	docs      map[string][]byte
	err       error
	requested []string
}

func (f *fakeFetcher) fetchSpec(_ context.Context, path string) ([]byte, error) {
	f.requested = append(f.requested, path)
	if f.err != nil {
		return nil, f.err
	}
	doc, found := f.docs[path]
	if !found {
		return nil, errors.New("no document at " + path)
	}
	return doc, nil
}

func newOpenAPIHandler(fetcher *fakeFetcher) *openAPIHandler {
	h := &openAPIHandler{
		cache:    fakeSnapshotter{snapshot: widgetSnapshot()},
		groups:   func() sets.Set[string] { return sets.New(testGroup) },
		fetcher:  func(string) specFetcher { return fetcher },
		fetchers: func() []namedFetcher { return []namedFetcher{{name: "test", fetcher: fetcher}} },
	}
	h.prepare()
	return h
}

// The index must expose the owned groups and nothing else: forwarding kcp's
// whole index would advertise kcp's own APIs to the workload cluster.
func TestOpenAPIDiscoveryFiltersToOwnedGroups(t *testing.T) {
	index, err := json.Marshal(handler3.OpenAPIV3Discovery{Paths: map[string]handler3.OpenAPIV3DiscoveryGroupVersion{
		"apis/" + testGroup + "/v1alpha1": {ServerRelativeURL: "/openapi/v3/apis/" + testGroup + "/v1alpha1?hash=abc"},
		"apis/tenancy.kcp.io/v1alpha1":    {ServerRelativeURL: "/openapi/v3/apis/tenancy.kcp.io/v1alpha1?hash=def"},
		"api/v1":                          {ServerRelativeURL: "/openapi/v3/api/v1?hash=ghi"},
	}})
	if err != nil {
		t.Fatalf("building the fake index: %v", err)
	}

	h := newOpenAPIHandler(&fakeFetcher{docs: map[string][]byte{"/openapi/v3": index}})
	recorder := get(t, h.serveDiscovery, "/openapi/v3")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body)
	}

	var got handler3.OpenAPIV3Discovery
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}

	if len(got.Paths) != 1 {
		t.Fatalf("got %d paths, want 1: %+v", len(got.Paths), got.Paths)
	}
	entry, found := got.Paths["apis/"+testGroup+"/v1alpha1"]
	if !found {
		t.Fatalf("the owned group version is missing: %+v", got.Paths)
	}
	if !strings.Contains(entry.ServerRelativeURL, "hash=abc") {
		t.Errorf("ServerRelativeURL = %q, want kcp's hash preserved for cache busting", entry.ServerRelativeURL)
	}
}

// A version the workspace no longer serves must drop out of the index even if
// kcp still lists it.
func TestOpenAPIDiscoveryDropsUnservedVersions(t *testing.T) {
	index, err := json.Marshal(handler3.OpenAPIV3Discovery{Paths: map[string]handler3.OpenAPIV3DiscoveryGroupVersion{
		"apis/" + testGroup + "/v1beta1": {ServerRelativeURL: "/openapi/v3/apis/" + testGroup + "/v1beta1"},
	}})
	if err != nil {
		t.Fatalf("building the fake index: %v", err)
	}

	h := newOpenAPIHandler(&fakeFetcher{docs: map[string][]byte{"/openapi/v3": index}})
	recorder := get(t, h.serveDiscovery, "/openapi/v3")

	var got handler3.OpenAPIV3Discovery
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}
	if len(got.Paths) != 0 {
		t.Errorf("got %+v, want no paths: the snapshot does not serve v1beta1", got.Paths)
	}
}

func TestOpenAPISpecIsProxied(t *testing.T) {
	const path = "/openapi/v3/apis/" + testGroup + "/v1alpha1"
	spec := []byte(`{"openapi":"3.0.0","info":{"title":"widgets"}}`)

	fetcher := &fakeFetcher{docs: map[string][]byte{path: spec}}
	h := newOpenAPIHandler(fetcher)
	recorder := get(t, h.serveSpec, path+"?hash=abc")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body)
	}
	if recorder.Body.String() != string(spec) {
		t.Errorf("body = %s, want the document kcp returned", recorder.Body)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	// The hash is kcp's cache buster, not a selector, so it is not forwarded.
	if len(fetcher.requested) != 1 || fetcher.requested[0] != path {
		t.Errorf("fetched %v, want exactly [%s]", fetcher.requested, path)
	}
}

func TestOpenAPISpecForUnownedGroupIsNotFound(t *testing.T) {
	h := newOpenAPIHandler(&fakeFetcher{docs: map[string][]byte{}})
	recorder := get(t, h.serveSpec, "/openapi/v3/apis/tenancy.kcp.io/v1alpha1")

	if recorder.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404; body: %s", recorder.Code, recorder.Body)
	}
}

func TestOpenAPIUnavailableWhenKCPFails(t *testing.T) {
	h := newOpenAPIHandler(&fakeFetcher{err: errors.New("connection refused")})

	recorder := get(t, h.serveDiscovery, "/openapi/v3")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("index: status = %d, want 503; body: %s", recorder.Code, recorder.Body)
	}

	recorder = get(t, h.serveSpec, "/openapi/v3/apis/"+testGroup+"/v1alpha1")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("spec: status = %d, want 503; body: %s", recorder.Code, recorder.Body)
	}
}

// owns only inspects the first three segments, so a traversal appended to an
// owned group version would otherwise be fetched from kcp.
func TestOpenAPIRefusesNonCanonicalPaths(t *testing.T) {
	fetcher := &fakeFetcher{docs: map[string][]byte{}}
	h := newOpenAPIHandler(fetcher)

	for _, escape := range []string{
		"/openapi/v3/apis/" + testGroup + "/v1alpha1/../../../../api/v1",
		"/openapi/v3/apis/" + testGroup + "/v1alpha1/../../tenancy.kcp.io/v1alpha1",
	} {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://spillway"+escape, nil)
		req.URL.Path = escape

		h.serveSpec(recorder, req)

		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", escape, recorder.Code)
		}
		if len(fetcher.requested) != 0 {
			t.Fatalf("%s: fetched %v from kcp", escape, fetcher.requested)
		}
	}
}

// stubRoundTripper answers every request with a canned response.
type stubRoundTripper struct {
	got    *http.Request
	status int
	body   string
	err    error
}

func (t *stubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.got = req.Clone(req.Context())
	if t.err != nil {
		return nil, t.err
	}
	status := t.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(t.body)),
	}, nil
}

func testSpecFetcher(t *testing.T, rt http.RoundTripper) *backendClient {
	t.Helper()

	location, err := url.Parse("https://kcp.example:6443/clusters/root:spillway")
	if err != nil {
		t.Fatalf("parsing the location: %v", err)
	}
	return &backendClient{location: location, transport: rt}
}

// The workspace prefix has to be prepended here just as it is for a resource
// request, or the spec would be fetched from the shard root.
func TestSpecFetcherAddressesTheWorkspace(t *testing.T) {
	rt := &stubRoundTripper{body: `{"openapi":"3.0.0"}`}

	body, err := testSpecFetcher(t, rt).fetchSpec(context.Background(), "/openapi/v3/apis/g/v1")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(body) != `{"openapi":"3.0.0"}` {
		t.Errorf("body = %s, want the document kcp returned", body)
	}

	want := "/clusters/root:spillway/openapi/v3/apis/g/v1"
	if rt.got.URL.Path != want {
		t.Errorf("fetched %q, want %q", rt.got.URL.Path, want)
	}
	if accept := rt.got.Header.Get("Accept"); accept != "application/json" {
		t.Errorf("Accept = %q, want application/json", accept)
	}
}

// Without a request info the transport would file these under unknown/unknown,
// mixed in with the data path.
func TestSpecFetcherLabelsItsRequests(t *testing.T) {
	rt := &stubRoundTripper{body: "{}"}

	if _, err := testSpecFetcher(t, rt).fetchSpec(context.Background(), "/openapi/v3"); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	verb, resource := requestLabels(rt.got)
	if verb != "get" || resource != "openapi" {
		t.Errorf("metric labels = %q/%q, want get/openapi", verb, resource)
	}
}

func TestSpecFetcherReportsAnErrorStatus(t *testing.T) {
	rt := &stubRoundTripper{status: http.StatusServiceUnavailable, body: "nope"}

	if _, err := testSpecFetcher(t, rt).fetchSpec(context.Background(), "/openapi/v3"); err == nil {
		t.Error("a 503 from kcp was reported as a document")
	}
}

// The transport is shared with the proxy, so a failure here reaches the caller
// as an error rather than being swallowed -- which is what lets it count
// against the circuit breaker.
func TestSpecFetcherPropagatesTransportFailures(t *testing.T) {
	rt := &stubRoundTripper{err: errors.New("circuit breaker is open")}

	_, err := testSpecFetcher(t, rt).fetchSpec(context.Background(), "/openapi/v3")
	if err == nil || !strings.Contains(err.Error(), "circuit breaker") {
		t.Errorf("err = %v, want the transport failure to surface", err)
	}
}

// A group that appears in the workspace after spillway started must be served
// without a restart, which is the whole point of a wildcard. The handler cannot
// have been registered for it: the mux was set up before the group existed.
func TestServeDispatchesGroupsDiscoveredLater(t *testing.T) {
	const late = "gadgets.tenant.example.net"

	snapshot := widgetSnapshot()
	snapshot.Groups[late] = metav1.APIGroup{
		Name:     late,
		Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: late + "/v1", Version: "v1"}},
	}
	snapshot.Resources[schema.GroupVersion{Group: late, Version: "v1"}] = []metav1.APIResource{
		{Name: "gadgets", Kind: "Gadget", Namespaced: true},
	}

	h := &discoveryHandler{
		cache: fakeSnapshotter{snapshot: snapshot},
		owns:  func(group string) bool { return strings.HasSuffix(group, ".tenant.example.net") },
	}

	recorder := get(t, h.serve, "/apis/"+late)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body)
	}
	if !strings.Contains(recorder.Body.String(), late) {
		t.Errorf("body does not name the group: %s", recorder.Body)
	}

	if recorder := get(t, h.serve, "/apis/"+late+"/v1"); recorder.Code != http.StatusOK {
		t.Errorf("version discovery status = %d, want 200; body: %s", recorder.Code, recorder.Body)
	}
}

// The other half: one prefix handler now sees every request under /apis, so it
// has to refuse the groups that are not spillway's rather than answer for them.
func TestServeRefusesGroupsThatAreNotOurs(t *testing.T) {
	h := &discoveryHandler{
		cache: fakeSnapshotter{snapshot: widgetSnapshot()},
		owns:  func(group string) bool { return group == testGroup },
	}

	for _, path := range []string{"/apis/apps", "/apis/apps/v1", "/apis/apps/v1/deployments", "/apis/"} {
		if recorder := get(t, h.serve, path); recorder.Code != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404; body: %s", path, recorder.Code, recorder.Body)
		}
	}
}
