package kcp

import (
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// Spillway deliberately keeps serving the last good discovery snapshot when a
// refresh fails, so staleness is invisible from the served API alone. These
// metrics are how an operator tells "kcp is unreachable" apart from "the
// workspace genuinely stopped serving that group".
var (
	refreshTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "spillway",
			Subsystem:      "kcp_discovery",
			Name:           "refresh_total",
			Help:           "Number of kcp discovery refresh attempts, by workspace and result.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace", "result"},
	)

	refreshDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "spillway",
			Subsystem:      "kcp_discovery",
			Name:           "refresh_duration_seconds",
			Help:           "How long a kcp discovery refresh took, by workspace.",
			Buckets:        metrics.ExponentialBuckets(0.01, 2, 10),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace"},
	)

	lastSuccessTimestamp = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace: "spillway",
			Subsystem: "kcp_discovery",
			Name:      "last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful kcp discovery refresh, by workspace. The age of " +
				"this value is how stale that workspace's served discovery may be -- which is per " +
				"workspace, since one being current says nothing about another.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace"},
	)

	groupServed = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "spillway",
			Subsystem:      "kcp",
			Name:           "group_served",
			Help:           "1 if the kcp workspace serves a configured API group, 0 otherwise.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group"},
	)
)

// The data path: what spillway does on behalf of every client request. The
// generic apiserver_request_* metrics already count these requests as spillway
// served them; these measure the hop to kcp, which is what tells a slow kcp
// apart from a slow spillway.
var (
	proxyRequests = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "spillway",
			Subsystem:      "proxy",
			Name:           "requests_total",
			Help:           "Requests proxied to kcp, by verb, resource and the status kcp returned. A code of <error> means the request never got a response.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace", "verb", "resource", "code"},
	)

	proxyDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace: "spillway",
			Subsystem: "proxy",
			Name:      "duration_seconds",
			Help: "Time from sending a request to kcp until its response headers arrive. " +
				"Deliberately not the time to stream the body: a watch would otherwise " +
				"record its whole lifetime and swamp the distribution.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 14),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace", "verb", "resource"},
	)

	proxyErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "spillway",
			Subsystem:      "proxy",
			Name:           "errors_total",
			Help:           "Requests that never reached kcp or never got an answer, by reason: connection, timeout, canceled, or circuit_open.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace", "reason", "verb", "resource"},
	)

	proxyRetries = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "spillway",
			Subsystem:      "proxy",
			Name:           "retries_total",
			Help:           "Retries of safe requests after a connection level failure, by workspace. A rising rate means kcp is flapping even when clients see no errors.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace"},
	)

	apiServiceSyncs = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace: "spillway",
			Subsystem: "apiservice",
			Name:      "sync_total",
			Help: "APIService reconciliations, by result. A registrar that cannot write to the " +
				"cluster looks exactly like one with nothing to do, so this is the only way to tell.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	apiServicesManaged = metrics.NewGauge(
		&metrics.GaugeOpts{
			Namespace:      "spillway",
			Subsystem:      "apiservice",
			Name:           "managed",
			Help:           "APIServices spillway currently maintains for the group versions the workspace serves.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	credentialReloads = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace: "spillway",
			Subsystem: "kcp",
			Name:      "credential_reload_total",
			Help: "Attempts to pick up rotated kcp credentials, by result. Errors here are the warning " +
				"before the credentials in hand expire.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace", "result"},
	)

	workspaceReloads = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace: "spillway",
			Subsystem: "workspaces",
			Name:      "reload_total",
			Help: "Attempts to apply a re-read workspaces configuration, by result. An entry that " +
				"cannot be brought up leaves the previous one serving and is counted here, so a " +
				"configuration that looks applied but is not has somewhere to show.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	circuitState = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace: "spillway",
			Subsystem: "kcp",
			Name:      "circuit_state",
			Help: "1 for a workspace's circuit breaker being in this state, 0 for the others. Open means " +
				"spillway is failing that workspace's requests fast rather than queueing them against " +
				"an unresponsive kcp. Labelled by workspace because each one has its own breaker, and " +
				"one being open says nothing about the rest.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"workspace", "state"},
	)
)

// ObserveProxyRequest records one attempt to reach kcp on a client's behalf.
func ObserveProxyRequest(workspace, verb, resource, code string, duration time.Duration) {
	proxyRequests.WithLabelValues(workspace, verb, resource, code).Inc()
	proxyDuration.WithLabelValues(workspace, verb, resource).Observe(duration.Seconds())
}

// ObserveProxyError records a request that got no answer from kcp.
func ObserveProxyError(workspace, reason, verb, resource string) {
	proxyErrors.WithLabelValues(workspace, reason, verb, resource).Inc()
}

// ObserveProxyRetry records a retried request.
func ObserveProxyRetry(workspace string) {
	proxyRetries.WithLabelValues(workspace).Inc()
}

// ObserveAPIServiceSync records one reconciliation of the registered
// APIServices, and how many are being maintained afterwards.
func ObserveAPIServiceSync(result string, managed int) {
	apiServiceSyncs.WithLabelValues(result).Inc()
	if result == "success" {
		apiServicesManaged.Set(float64(managed))
	}
}

// ObserveCredentialReload records one attempt to re-read a workspace's
// kubeconfig.
func ObserveCredentialReload(workspace, result string) {
	credentialReloads.WithLabelValues(workspace, result).Inc()
}

// ObserveWorkspaceReload records one attempt to apply the workspaces
// configuration as it now reads.
func ObserveWorkspaceReload(result string) {
	workspaceReloads.WithLabelValues(result).Inc()
}

// SetCircuitState publishes the breaker's state, one series per state so a
// dashboard can graph the transition rather than decode an enum.
func SetCircuitState(workspace, state string) {
	for _, known := range []string{"closed", "open", "half-open"} {
		value := 0.0
		if known == state {
			value = 1
		}
		circuitState.WithLabelValues(workspace, known).Set(value)
	}
}

var registerOnce sync.Once

// PublishWorkspace creates the series for a workspace, so they exist from
// startup rather than appearing only once something breaks. Without this, the
// absence of a circuit_state series would be indistinguishable from a workspace
// whose circuit has never moved.
func PublishWorkspace(workspace string) {
	SetCircuitState(workspace, "closed")
}

// RegisterMetrics makes the spillway metrics visible on /metrics. It is safe to
// call more than once.
func RegisterMetrics() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(refreshTotal)
		legacyregistry.MustRegister(refreshDuration)
		legacyregistry.MustRegister(lastSuccessTimestamp)
		legacyregistry.MustRegister(groupServed)
		legacyregistry.MustRegister(proxyRequests)
		legacyregistry.MustRegister(proxyDuration)
		legacyregistry.MustRegister(proxyErrors)
		legacyregistry.MustRegister(proxyRetries)
		legacyregistry.MustRegister(circuitState)
		legacyregistry.MustRegister(apiServiceSyncs)
		legacyregistry.MustRegister(apiServicesManaged)
		legacyregistry.MustRegister(credentialReloads)
		legacyregistry.MustRegister(workspaceReloads)

	})
}
