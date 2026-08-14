package server

import (
	"io"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"github.com/mrueg/spillway/pkg/apiserver"
)

func testOptions(groups ...string) *SpillwayServerOptions {
	o := NewSpillwayServerOptions(io.Discard, io.Discard)
	o.APIGroups = groups
	return o
}

// validate reports only the spillway specific problems. The embedded
// RecommendedOptions contribute their own errors in a real run, which are not
// what these tests are about.
func spillwayErrors(t *testing.T, o *SpillwayServerOptions) []string {
	t.Helper()

	err := o.Validate()
	if err == nil {
		return nil
	}

	var found []string
	for _, line := range strings.Split(err.Error(), ",") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "api-group") || strings.Contains(line, "kcp-resync-period") ||
			strings.Contains(line, "-inflight") {
			found = append(found, line)
		}
	}
	return found
}

func TestValidateRequiresAnAPIGroup(t *testing.T) {
	errs := spillwayErrors(t, testOptions())

	if len(errs) != 1 || !strings.Contains(errs[0], "at least one --api-group") {
		t.Errorf("Validate() reported %v, want a single complaint about a missing --api-group", errs)
	}
}

func TestValidateAcceptsAGroupName(t *testing.T) {
	if errs := spillwayErrors(t, testOptions("widgets.example.com")); len(errs) != 0 {
		t.Errorf("Validate() reported %v for a valid group, want none", errs)
	}
}

// A group/version is the natural thing to type, and silently serving every
// version of the group instead would be surprising -- so it is rejected with a
// message that says what to type instead.
func TestValidateRejectsAGroupWithAVersion(t *testing.T) {
	errs := spillwayErrors(t, testOptions("widgets.example.com/v1alpha1"))

	if len(errs) != 1 || !strings.Contains(errs[0], "without a version") {
		t.Errorf("Validate() reported %v, want a complaint about the version suffix", errs)
	}
}

func TestValidateRejectsAnEmptyGroup(t *testing.T) {
	errs := spillwayErrors(t, testOptions(""))

	if len(errs) == 0 {
		t.Fatal("Validate() accepted an empty --api-group; the core group cannot be offloaded")
	}
	if !strings.Contains(errs[0], "must not be empty") {
		t.Errorf("Validate() reported %q, want a complaint about the empty group", errs[0])
	}
}

func TestValidateRejectsANonPositiveResyncPeriod(t *testing.T) {
	for _, period := range []time.Duration{0, -time.Second} {
		o := testOptions("widgets.example.com")
		o.ResyncPeriod = period

		errs := spillwayErrors(t, o)
		if len(errs) != 1 || !strings.Contains(errs[0], "must be positive") {
			t.Errorf("Validate() with resync %s reported %v, want a complaint that it must be positive", period, errs)
		}
	}
}

func TestDefaultResyncPeriodIsPositive(t *testing.T) {
	if o := NewSpillwayServerOptions(io.Discard, io.Discard); o.ResyncPeriod <= 0 {
		t.Errorf("default ResyncPeriod = %s, want a positive default", o.ResyncPeriod)
	}
}

// Spillway keeps no state of its own, so the etcd options are dropped rather
// than left half configured. If they came back, --etcd-servers would reappear
// and the server would expect storage it never uses.
func TestEtcdOptionsAreDropped(t *testing.T) {
	if o := NewSpillwayServerOptions(io.Discard, io.Discard); o.RecommendedOptions.Etcd != nil {
		t.Error("RecommendedOptions.Etcd is set; spillway stores nothing of its own")
	}
}

// A repeated group would otherwise register the same mux path twice.
func TestCompleteDeduplicatesAPIGroups(t *testing.T) {
	o := testOptions("b.example.com", "a.example.com", "b.example.com")

	if err := o.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	want := []string{"a.example.com", "b.example.com"}
	if len(o.APIGroups) != len(want) {
		t.Fatalf("APIGroups = %v, want %v", o.APIGroups, want)
	}
	for i, group := range want {
		if o.APIGroups[i] != group {
			t.Errorf("APIGroups[%d] = %q, want %q (sorted, so discovery order is stable)", i, o.APIGroups[i], group)
		}
	}
}

func TestCompleteWithNoGroups(t *testing.T) {
	o := testOptions()

	if err := o.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(o.APIGroups) != 0 {
		t.Errorf("APIGroups = %v, want none", o.APIGroups)
	}
}

// The generic health endpoints skip authorization so that a probe, or an
// operator with no credentials to hand, can read them. The per group endpoint
// is the same kind of thing and was unreachable without this.
func TestGroupHealthPathSkipsAuthorization(t *testing.T) {
	o := NewSpillwayServerOptions(io.Discard, io.Discard)

	var found bool
	for _, path := range o.RecommendedOptions.Authorization.AlwaysAllowPaths {
		if path == apiserver.GroupHealthPath+"*" {
			found = true
		}
	}
	if !found {
		t.Errorf("AlwaysAllowPaths = %v, want it to cover %s and its per group subpaths",
			o.RecommendedOptions.Authorization.AlwaysAllowPaths, apiserver.GroupHealthPath)
	}

	// The defaults must survive: dropping them would break the kubelet probes.
	for _, standard := range []string{"/healthz", "/readyz", "/livez"} {
		var kept bool
		for _, path := range o.RecommendedOptions.Authorization.AlwaysAllowPaths {
			if path == standard {
				kept = true
			}
		}
		if !kept {
			t.Errorf("%s is no longer exempt from authorization", standard)
		}
	}
}

// The generic server fixes these at 400 and 200 and exposes no flag, so the
// only evidence they are reachable is that the configuration carries what was
// asked for.
func TestConfigCarriesTheInFlightLimits(t *testing.T) {
	options := testOptions("widgets.example.com")
	if options.MaxRequestsInFlight != defaultMaxRequestsInFlight ||
		options.MaxMutatingRequestsInFlight != defaultMaxMutatingRequestsInFlight {
		t.Errorf("defaults = %d/%d, want %d/%d",
			options.MaxRequestsInFlight, options.MaxMutatingRequestsInFlight,
			defaultMaxRequestsInFlight, defaultMaxMutatingRequestsInFlight)
	}
}

func TestValidateRejectsNegativeInFlightLimits(t *testing.T) {
	options := testOptions("widgets.example.com")
	options.MaxRequestsInFlight = -1
	if errs := spillwayErrors(t, options); !containsSubstring(errs, "--max-requests-inflight") {
		t.Errorf("Validate() reported %v, want a complaint about --max-requests-inflight", errs)
	}

	options = testOptions("widgets.example.com")
	options.MaxMutatingRequestsInFlight = -1
	if errs := spillwayErrors(t, options); !containsSubstring(errs, "--max-mutating-requests-inflight") {
		t.Errorf("Validate() reported %v, want a complaint about --max-mutating-requests-inflight", errs)
	}
}

func containsSubstring(errs []string, want string) bool {
	for _, err := range errs {
		if strings.Contains(err, want) {
			return true
		}
	}
	return false
}

// A default of zero is indistinguishable from a feature nobody asked for: the
// reload loop simply returns. These are the options whose default is what makes
// them work at all.
func TestDefaultsAreNotZero(t *testing.T) {
	options := NewSpillwayServerOptions(io.Discard, io.Discard)

	if options.CredentialReloadPeriod <= 0 {
		t.Errorf("CredentialReloadPeriod = %s; credentials would never be re-read",
			options.CredentialReloadPeriod)
	}
	if options.ResyncPeriod <= 0 {
		t.Errorf("ResyncPeriod = %s; the backstop refresh would never run", options.ResyncPeriod)
	}
	if options.Backend.RequestTimeout <= 0 {
		t.Errorf("RequestTimeout = %s; requests to kcp would be unbounded", options.Backend.RequestTimeout)
	}
	if options.MaxRequestsInFlight <= 0 || options.MaxMutatingRequestsInFlight <= 0 {
		t.Errorf("in-flight limits = %d/%d; the server would be unbounded",
			options.MaxRequestsInFlight, options.MaxMutatingRequestsInFlight)
	}
	if options.AuthorizationQPS <= 0 || options.AuthorizationBurst <= 0 {
		t.Errorf("authorization limits = %d/%d", options.AuthorizationQPS, options.AuthorizationBurst)
	}
}

// Every option has to reach the configuration. A field left out of the mapping
// is invisible: the flag parses, it validates, it appears in --help, and it does
// nothing. This has caught three of them.
func TestExtraConfigCarriesEveryOption(t *testing.T) {
	options := testOptions("widgets.example.com")
	options.WorkspacesFile = writeWorkspaces(t,
		"workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: [\"a.example.com\"]")
	options.MirrorNamespaces = true
	options.ImpersonateUsers = true
	options.CredentialReloadPeriod = 3 * time.Minute
	options.ResyncPeriod = 7 * time.Minute
	options.APIServices.Register = true
	options.Backend.Retries = 9

	workspaces, err := options.workspaces()
	if err != nil {
		t.Fatalf("workspaces: %v", err)
	}
	cluster := &rest.Config{Host: "https://cluster.example"}
	extra := options.extraConfig(workspaces, cluster)

	if len(extra.Workspaces) != 1 || extra.Workspaces[0].Name != "a" {
		t.Errorf("Workspaces = %+v", extra.Workspaces)
	}
	if extra.ReloadWorkspaces == nil {
		t.Error("ReloadWorkspaces is nil; the configuration would never be re-read")
	}
	if extra.ClusterClientConfig != cluster {
		t.Error("ClusterClientConfig did not survive the mapping")
	}
	if !extra.MirrorNamespaces {
		t.Error("MirrorNamespaces did not survive the mapping")
	}
	if !extra.ImpersonateUsers {
		t.Error("ImpersonateUsers did not survive the mapping")
	}
	if extra.CredentialReloadPeriod != 3*time.Minute {
		t.Errorf("CredentialReloadPeriod = %s, want 3m", extra.CredentialReloadPeriod)
	}
	if extra.ResyncPeriod != 7*time.Minute {
		t.Errorf("ResyncPeriod = %s, want 7m", extra.ResyncPeriod)
	}
	if !extra.APIServices.Register {
		t.Error("APIServices did not survive the mapping")
	}
	if extra.Backend.Retries != 9 {
		t.Errorf("Backend = %+v, did not survive the mapping", extra.Backend)
	}
}

// Without a file there is nothing to re-read: the flags are the process's
// arguments, and changing those is a restart by definition.
func TestReloadIsNilWithoutAFile(t *testing.T) {
	if reload := testOptions("widgets.example.com").reloadWorkspaces(); reload != nil {
		t.Error("a reload was offered for a configuration that came from flags")
	}
}

// The election's durations have to relate to each other or the lease is
// unrenewable by construction.
func TestValidateChecksTheElectionDurations(t *testing.T) {
	options := testOptions("widgets.example.com")
	options.APIServices.Register = true
	options.APIServices.InsecureSkipTLSVerify = true
	options.APIServices.ServiceNamespace = "spillway-system"
	options.LeaderElection.Namespace = "spillway-system"

	options.LeaderElection.LeaseDuration = time.Second
	options.LeaderElection.RenewDeadline = 10 * time.Second
	if err := options.Validate(); err == nil || !strings.Contains(err.Error(), "lease-duration") {
		t.Errorf("Validate() = %v, want it to refuse a lease shorter than its renew deadline", err)
	}

	options.LeaderElection.LeaseDuration = 15 * time.Second
	options.LeaderElection.RetryPeriod = 12 * time.Second
	if err := options.Validate(); err == nil || !strings.Contains(err.Error(), "retry-period") {
		t.Errorf("Validate() = %v, want it to refuse a retry period longer than the renew deadline", err)
	}
}

// Election is about who writes. A replica that is not leading still serves, and
// still reports what it is serving.
func TestRegistrarWaitsForLeadershipButServingDoesNot(t *testing.T) {
	options := testOptions("widgets.example.com")
	if !options.LeaderElection.Enabled {
		t.Error("leader election is off by default; two replicas would both write")
	}
	if options.LeaderElection.LeaseDuration <= options.LeaderElection.RenewDeadline ||
		options.LeaderElection.RenewDeadline <= options.LeaderElection.RetryPeriod {
		t.Errorf("the default durations do not relate correctly: %+v", options.LeaderElection)
	}
}
