package kcp

import (
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
)

// One breaker per workspace writes to one gauge, so without a workspace label
// the last transition wins and an open circuit is hidden by any other workspace
// moving afterwards. This is the shape of the bug, as a test.
func TestCircuitStateIsPerWorkspace(t *testing.T) {
	RegisterMetrics()

	SetCircuitState("alpha", "open")
	SetCircuitState("beta", "closed")

	if got := gaugeValue(t, "spillway_kcp_circuit_state", map[string]string{"workspace": "alpha", "state": "open"}); got != 1 {
		t.Errorf("alpha open = %v, want 1: beta closing must not clear it", got)
	}
	if got := gaugeValue(t, "spillway_kcp_circuit_state", map[string]string{"workspace": "beta", "state": "closed"}); got != 1 {
		t.Errorf("beta closed = %v, want 1", got)
	}
	if got := gaugeValue(t, "spillway_kcp_circuit_state", map[string]string{"workspace": "alpha", "state": "closed"}); got != 0 {
		t.Errorf("alpha closed = %v, want 0", got)
	}
}

// The staleness of one workspace's discovery says nothing about another's, so
// the timestamp cannot be a single series either.
func TestLastSuccessIsPerWorkspace(t *testing.T) {
	RegisterMetrics()

	lastSuccessTimestamp.WithLabelValues("alpha").Set(1000)
	lastSuccessTimestamp.WithLabelValues("beta").Set(2000)

	if got := gaugeValue(t, "spillway_kcp_discovery_last_success_timestamp_seconds",
		map[string]string{"workspace": "alpha"}); got != 1000 {
		t.Errorf("alpha = %v, want 1000: beta refreshing must not make alpha look current", got)
	}
}

// gaugeValue reads one series out of the registry the server serves.
func gaugeValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := 0
			for _, pair := range metric.GetLabel() {
				if labels[pair.GetName()] == pair.GetValue() {
					matched++
				}
			}
			if matched == len(labels) {
				return metric.GetGauge().GetValue()
			}
		}
	}
	return -1
}
