package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/mrueg/spillway/pkg/kcp"
)

// BackendOptions tune how spillway talks to kcp.
type BackendOptions struct {
	// RequestTimeout bounds a single request to kcp. Watches are exempt: they
	// are long lived by design, and are bounded by ResponseHeaderTimeout on the
	// transport instead, which covers a kcp that accepts a connection and then
	// never answers.
	RequestTimeout time.Duration

	// Retries is how many times a safe, idempotent request is retried after a
	// connection level failure. Requests that change state are never retried.
	Retries int

	// FailureThreshold is how many consecutive backend failures open the
	// circuit.
	FailureThreshold int

	// CircuitCooldown is how long the circuit stays open before a single probe
	// is allowed through.
	CircuitCooldown time.Duration
}

// errCircuitOpen is returned instead of dialing kcp while the circuit is open.
var errCircuitOpen = errors.New("circuit breaker is open: kcp has been failing, so requests fail fast instead of piling up")

// circuitState is the breaker's state machine.
//
// closed lets everything through. Enough consecutive failures open it, and
// while open every request fails immediately rather than queueing against a kcp
// that is not answering -- which is what turns a slow backend into an
// unavailable APIService. After the cooldown a single probe decides whether to
// close again.
type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

func (s circuitState) String() string {
	switch s {
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

type breaker struct {
	workspace string
	threshold int
	cooldown  time.Duration

	mu        sync.Mutex
	state     circuitState
	failures  int
	openUntil time.Time
	probing   bool
}

func newBreaker(workspace string, threshold int, cooldown time.Duration) *breaker {
	return &breaker{workspace: workspace, threshold: threshold, cooldown: cooldown}
}

// open reports whether the circuit is currently refusing requests, for the
// health endpoint. It does not advance the state machine: asking how spillway
// is does not count as a probe.
func (b *breaker) open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == circuitOpen
}

// allow reports whether a request may be sent to kcp.
func (b *breaker) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case circuitOpen:
		if now.Before(b.openUntil) {
			return false
		}
		b.setState(circuitHalfOpen)
		b.probing = true
		return true
	case circuitHalfOpen:
		// One probe at a time decides whether kcp is back.
		if b.probing {
			return false
		}
		b.probing = true
		return true
	default:
		return true
	}
}

// record folds the outcome of a request into the breaker.
func (b *breaker) record(succeeded bool, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if succeeded {
		b.failures = 0
		b.probing = false
		b.setState(circuitClosed)
		return
	}

	b.failures++
	// A failed probe reopens immediately; there is no point waiting for the
	// threshold again when kcp has just told us it is still down.
	if b.state == circuitHalfOpen || b.failures >= b.threshold {
		b.failures = 0
		b.probing = false
		b.openUntil = now.Add(b.cooldown)
		b.setState(circuitOpen)
	}
}

// setState must be called with the lock held.
func (b *breaker) setState(next circuitState) {
	if b.state != next {
		b.state = next
		kcp.SetCircuitState(b.workspace, next.String())
	}
}

// backendTransport wraps the transport to kcp with the circuit breaker, retries
// for safe requests, and the metrics for the proxy data path.
type backendTransport struct {
	workspace string

	base    http.RoundTripper
	breaker *breaker
	retries int

	// now is injected so the breaker's timing can be tested.
	now func() time.Time
}

func newBackendTransport(workspace string, base http.RoundTripper, options BackendOptions) *backendTransport {
	return &backendTransport{
		workspace: workspace,
		base:      base,
		breaker:   newBreaker(workspace, options.FailureThreshold, options.CircuitCooldown),
		retries:   options.Retries,
		now:       time.Now,
	}
}

func (t *backendTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	verb, resource := requestLabels(req)

	if !t.breaker.allow(t.now()) {
		kcp.ObserveProxyError(t.workspace, "circuit_open", verb, resource)
		return nil, errCircuitOpen
	}

	started := t.now()
	resp, err := t.roundTripWithRetries(req)
	kcp.ObserveProxyRequest(t.workspace, verb, resource, statusLabel(resp, err), t.now().Sub(started))

	// A 5xx from kcp counts against the backend the same as a refused
	// connection. A 4xx does not: that is the caller's request being wrong.
	succeeded := err == nil && resp.StatusCode < http.StatusInternalServerError
	t.breaker.record(succeeded, t.now())

	if err != nil {
		kcp.ObserveProxyError(t.workspace, errorReason(req.Context(), err), verb, resource)
	}
	return resp, err
}

func (t *backendTransport) roundTripWithRetries(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err == nil || !retriable(req) {
		return resp, err
	}

	for attempt := range t.retries {
		if req.Context().Err() != nil {
			return resp, err
		}
		// A short, growing pause: the failures worth retrying are a connection
		// closed mid-flight or a shard rolling, not a kcp that is down.
		backoff := time.Duration(attempt+1) * 50 * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return resp, err
		case <-timer.C:
		}

		kcp.ObserveProxyRetry(t.workspace)
		if resp, err = t.base.RoundTrip(req); err == nil {
			return resp, nil
		}
	}
	return resp, err
}

// retriable reports whether replaying the request is safe. Only requests that
// neither change state nor carry a body qualify: retrying a create would risk
// making the object twice.
func retriable(req *http.Request) bool {
	if req.Body != nil && req.Body != http.NoBody {
		return false
	}
	return req.Method == http.MethodGet || req.Method == http.MethodHead
}

// requestLabels names the request for metrics. Only the verb and resource are
// used: a namespace or object name would make the series unbounded.
func requestLabels(req *http.Request) (verb, resource string) {
	info, found := genericapirequest.RequestInfoFrom(req.Context())
	if !found {
		return "unknown", "unknown"
	}
	verb, resource = info.Verb, info.Resource
	if verb == "" {
		verb = "unknown"
	}
	if resource == "" {
		resource = "unknown"
	}
	return verb, resource
}

func statusLabel(resp *http.Response, err error) string {
	if err != nil || resp == nil {
		return "<error>"
	}
	return fmt.Sprintf("%d", resp.StatusCode)
}

// errorReason buckets a failure so the metric says what went wrong without
// carrying the error text, which would be unbounded.
func errorReason(ctx context.Context, err error) string {
	switch {
	case errors.Is(err, errCircuitOpen):
		return "circuit_open"
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		return "canceled"
	default:
		return "connection"
	}
}
