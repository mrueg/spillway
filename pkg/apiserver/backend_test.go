package apiserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

var errBackend = errors.New("connection refused")

// scriptedTransport returns the given outcomes in order, repeating the last.
type scriptedTransport struct {
	outcomes []error
	calls    int
	status   int
}

func (t *scriptedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	index := min(t.calls, len(t.outcomes)-1)
	t.calls++

	if err := t.outcomes[index]; err != nil {
		return nil, err
	}
	status := t.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("{}")),
	}, nil
}

func backendUnder(t *testing.T, base http.RoundTripper, options BackendOptions, clock func() time.Time) *backendTransport {
	t.Helper()

	transport := newBackendTransport("test", base, options)
	if clock != nil {
		transport.now = clock
	}
	return transport
}

func request(method, path string) *http.Request {
	return httptest.NewRequest(method, path, nil)
}

func TestRetriesASafeRequest(t *testing.T) {
	base := &scriptedTransport{outcomes: []error{errBackend, errBackend, nil}}
	transport := backendUnder(t, base, BackendOptions{Retries: 2, FailureThreshold: 99, CircuitCooldown: time.Minute}, nil)

	resp, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets"))
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if base.calls != 3 {
		t.Errorf("kcp was called %d times, want 3 (the original plus two retries)", base.calls)
	}
}

// Replaying a create would risk making the object twice, so anything that
// changes state gets exactly one attempt.
func TestDoesNotRetryWritingRequests(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		base := &scriptedTransport{outcomes: []error{errBackend}}
		transport := backendUnder(t, base, BackendOptions{Retries: 3, FailureThreshold: 99, CircuitCooldown: time.Minute}, nil)

		if _, err := transport.RoundTrip(request(method, "/apis/g/v1/widgets")); err == nil {
			t.Fatalf("%s: expected the failure to surface", method)
		}
		if base.calls != 1 {
			t.Errorf("%s was attempted %d times, want exactly 1", method, base.calls)
		}
	}
}

func TestGivesUpAfterTheRetryBudget(t *testing.T) {
	base := &scriptedTransport{outcomes: []error{errBackend}}
	transport := backendUnder(t, base, BackendOptions{Retries: 2, FailureThreshold: 99, CircuitCooldown: time.Minute}, nil)

	if _, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets")); !errors.Is(err, errBackend) {
		t.Fatalf("err = %v, want the backend error", err)
	}
	if base.calls != 3 {
		t.Errorf("kcp was called %d times, want 3", base.calls)
	}
}

// The point of the breaker: once kcp is clearly down, stop dialling it.
func TestCircuitOpensAfterConsecutiveFailures(t *testing.T) {
	base := &scriptedTransport{outcomes: []error{errBackend}}
	transport := backendUnder(t, base, BackendOptions{Retries: 0, FailureThreshold: 3, CircuitCooldown: time.Minute}, nil)

	for range 3 {
		if _, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets")); err == nil {
			t.Fatal("expected the backend failure to surface")
		}
	}
	attemptsBeforeOpen := base.calls

	_, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets"))
	if !errors.Is(err, errCircuitOpen) {
		t.Fatalf("err = %v, want the circuit to be open", err)
	}
	if base.calls != attemptsBeforeOpen {
		t.Errorf("kcp was dialled while the circuit was open (%d calls, was %d)", base.calls, attemptsBeforeOpen)
	}
}

func TestCircuitProbesAfterTheCooldownAndCloses(t *testing.T) {
	base := &scriptedTransport{outcomes: []error{errBackend, errBackend, nil}}

	now := time.Now()
	clock := func() time.Time { return now }
	transport := backendUnder(t, base, BackendOptions{Retries: 0, FailureThreshold: 2, CircuitCooldown: 30 * time.Second}, clock)

	for range 2 {
		_, _ = transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets")) //nolint:bodyclose // no body on error
	}
	if _, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets")); !errors.Is(err, errCircuitOpen) {
		t.Fatal("the circuit should be open")
	}

	// Once the cooldown has passed a single probe is allowed, and kcp is well
	// again, so the circuit closes.
	now = now.Add(31 * time.Second)

	resp, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets"))
	if err != nil {
		t.Fatalf("the probe failed: %v", err)
	}
	resp.Body.Close()

	resp, err = transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets"))
	if err != nil {
		t.Fatalf("the circuit did not close after a successful probe: %v", err)
	}
	resp.Body.Close()
}

// A 5xx is the backend failing; a 4xx is the caller's request being wrong and
// must not trip the breaker on everyone else's behalf.
func TestClientErrorsDoNotOpenTheCircuit(t *testing.T) {
	base := &scriptedTransport{outcomes: []error{nil}, status: http.StatusNotFound}
	transport := backendUnder(t, base, BackendOptions{Retries: 0, FailureThreshold: 2, CircuitCooldown: time.Minute}, nil)

	for range 5 {
		resp, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets"))
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		resp.Body.Close()
	}

	if base.calls != 5 {
		t.Errorf("kcp was called %d times, want 5: 404s must not open the circuit", base.calls)
	}
}

func TestServerErrorsOpenTheCircuit(t *testing.T) {
	base := &scriptedTransport{outcomes: []error{nil}, status: http.StatusInternalServerError}
	transport := backendUnder(t, base, BackendOptions{Retries: 0, FailureThreshold: 2, CircuitCooldown: time.Minute}, nil)

	for range 2 {
		resp, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets"))
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		resp.Body.Close()
	}

	if _, err := transport.RoundTrip(request(http.MethodGet, "/apis/g/v1/widgets")); !errors.Is(err, errCircuitOpen) {
		t.Errorf("err = %v, want the circuit to open after repeated 5xx from kcp", err)
	}
}

// A watch streams for as long as the client wants it; a deadline would cut it
// off mid stream and look like a broken API to every controller.
func TestWatchIsRecognised(t *testing.T) {
	watch := request(http.MethodGet, "/apis/g/v1/widgets?watch=true")
	if !isWatch(watch) {
		t.Error("a request with watch=true was not recognised as a watch")
	}

	fromInfo := request(http.MethodGet, "/apis/g/v1/widgets")
	fromInfo = fromInfo.WithContext(genericapirequest.WithRequestInfo(
		fromInfo.Context(), &genericapirequest.RequestInfo{Verb: "watch"}))
	if !isWatch(fromInfo) {
		t.Error("a request whose RequestInfo says watch was not recognised as a watch")
	}

	list := request(http.MethodGet, "/apis/g/v1/widgets")
	list = list.WithContext(genericapirequest.WithRequestInfo(
		list.Context(), &genericapirequest.RequestInfo{Verb: "list"}))
	if isWatch(list) {
		t.Error("a list was mistaken for a watch, so it would never time out")
	}
}

func TestRequestLabelsAreBounded(t *testing.T) {
	// Without a RequestInfo there is nothing to name the series after, and the
	// URL must not be used: it carries object names.
	verb, resource := requestLabels(request(http.MethodGet, "/apis/g/v1/namespaces/n/widgets/my-widget"))
	if verb != "unknown" || resource != "unknown" {
		t.Errorf("labels = %q/%q, want unknown/unknown", verb, resource)
	}

	withInfo := request(http.MethodGet, "/apis/g/v1/namespaces/n/widgets/my-widget")
	withInfo = withInfo.WithContext(genericapirequest.WithRequestInfo(withInfo.Context(),
		&genericapirequest.RequestInfo{Verb: "get", Resource: "widgets", Name: "my-widget", Namespace: "n"}))

	verb, resource = requestLabels(withInfo)
	if verb != "get" || resource != "widgets" {
		t.Errorf("labels = %q/%q, want get/widgets", verb, resource)
	}
	if strings.Contains(verb+resource, "my-widget") {
		t.Error("the object name leaked into a metric label")
	}
}

// The health endpoint asks the breaker what it is doing. Asking must not count
// as a probe: a scrape every few seconds would otherwise keep letting requests
// through to a kcp that is down.
func TestBreakerOpenDoesNotAdvanceTheStateMachine(t *testing.T) {
	b := newBreaker("test", 2, time.Minute)
	now := time.Now()

	if b.open() {
		t.Error("a fresh breaker reports open")
	}

	b.record(false, now)
	b.record(false, now)
	if !b.open() {
		t.Fatal("the breaker did not open after reaching the failure threshold")
	}

	// Past the cooldown one request is let through to test kcp. Asking about
	// the state must not be that request.
	later := now.Add(2 * time.Minute)
	for range 5 {
		b.open()
	}
	if !b.allow(later) {
		t.Error("the probe was consumed by the health endpoint asking about the state")
	}
	if b.allow(later) {
		t.Error("a second request was let through while the first probe is outstanding")
	}
}

// sideCallTransport captures the request a side call actually sent.
type sideCallTransport struct {
	seen *http.Request
}

func (t *sideCallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.seen = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{}`)),
		Header:     http.Header{},
	}, nil
}

// The dry run has to ask kcp the same question the caller asked, plus dryRun.
// Dropping fieldManager makes an apply unresolvable, and an unresolvable patch
// is forwarded unadmitted -- so this is what stands between a server-side apply
// and skipping every webhook.
func TestPatchDryRunKeepsTheCallersQuery(t *testing.T) {
	transport := &sideCallTransport{}
	location, err := url.Parse("https://kcp.example:6443/clusters/root:spillway")
	if err != nil {
		t.Fatalf("parsing the location: %v", err)
	}
	client := &backendClient{location: location, transport: transport}

	caller := url.Values{
		"fieldManager":    []string{"kubectl"},
		"fieldValidation": []string{"Strict"},
		"force":           []string{"true"},
	}
	if _, err := client.patchDryRun(context.Background(),
		"/apis/g/v1alpha1/namespaces/default/widgets/one", caller,
		"application/apply-patch+yaml", []byte(`{"spec":{"size":1}}`)); err != nil {
		t.Fatalf("patchDryRun: %v", err)
	}

	sent := transport.seen.URL.Query()
	if sent.Get("dryRun") != "All" {
		t.Errorf("dryRun = %q, want All", sent.Get("dryRun"))
	}
	for parameter, want := range map[string]string{
		"fieldManager":    "kubectl",
		"fieldValidation": "Strict",
		"force":           "true",
	} {
		if got := sent.Get(parameter); got != want {
			t.Errorf("%s = %q on the dry run, want %q", parameter, got, want)
		}
	}

	// And the caller's own map is not modified on the way past.
	if caller.Has("dryRun") {
		t.Error("the caller's query was mutated")
	}
}
