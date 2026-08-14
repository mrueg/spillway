//go:build bench

package bench

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// report prints the comparison. It is written for a human reading the test
// output, not for a machine, because the numbers only mean anything alongside
// what they were measured on.
func report(t *testing.T, backends []backend, rows []phaseResult,
	watchLatency map[string]*series, storageBefore, storagePeak map[string]int,
	authorizations int, unrelated map[string]*series) {
	t.Helper()

	var out strings.Builder
	line := func(format string, args ...any) {
		fmt.Fprintf(&out, format+"\n", args...)
	}

	line("")
	line("%d objects, %d workers, %d reads, %d lists, %d watch events, %d runs, after a %d object warmup",
		objects, concurrency, reads, lists, watches, runs, warmup)
	line("")
	line("medians across runs; the range is the spread of that run's p50")
	line("")
	line("%-8s  %-18s  %10s  %9s  %-21s  %9s  %7s", "phase", "backend", "ops/sec", "p50", "p50 range", "p95", "errors")
	line("%s", strings.Repeat("-", 94))

	for _, row := range rows {
		for _, target := range backends {
			result := row.result[target.name]
			if result == nil || len(result.runs) == 0 {
				continue
			}
			median, low, high := result.percentileAcrossRuns(0.50)
			p95, _, _ := result.percentileAcrossRuns(0.95)
			line("%-8s  %-18s  %10.1f  %9s  %-21s  %9s  %7d",
				row.phase, target.name, result.medianThroughput(), round(median),
				round(low)+" - "+round(high), round(p95), result.failures())
		}
		line("")
	}

	line("%-8s  %-18s  %10s  %9s  %-21s  %9s  %7s", "watch", "backend", "events/s", "p50", "p50 range", "p95", "errors")
	line("%s", strings.Repeat("-", 94))
	for _, target := range backends {
		result := watchLatency[target.name]
		if result == nil || len(result.runs) == 0 {
			continue
		}
		median, low, high := result.percentileAcrossRuns(0.50)
		p95, _, _ := result.percentileAcrossRuns(0.95)
		line("%-8s  %-18s  %10.1f  %9s  %-21s  %9s  %7d",
			"notify", target.name, result.medianThroughput(), round(median),
			round(low)+" - "+round(high), round(p95), result.failures())
	}

	line("")
	line("unrelated traffic, while each backend's objects were being created")
	line("  one ConfigMap written and deleted every 200ms, which has nothing to do with the workload")
	line("----------------------------------------------------------------------------------")
	line("  %-20s %10s %10s %8s", "during", "p50", "p95", "samples")
	for _, name := range []string{"idle", "native CRD", "kcp via spillway"} {
		measured, found := unrelated[name]
		if !found || len(measured.runs) == 0 {
			continue
		}
		median, _, _ := measured.percentileAcrossRuns(0.50)
		p95, _, _ := measured.percentileAcrossRuns(0.95)
		samples := 0
		for _, run := range measured.runs {
			samples += len(run.durations)
		}
		line("  %-20s %10s %10s %8d", name, median.Round(time.Millisecond), p95.Round(time.Millisecond), samples)
	}

	line("")
	line("SubjectAccessReviews the cluster served during the run: %d", authorizations)
	line("  spillway asks the cluster to authorize each request its cache has not seen, and the")
	line("  cache keys on the object name, so distinct objects miss. An admin identity skips this")
	line("  entirely: system:masters is short-circuited before the check.")

	line("")
	line("objects the cluster's own apiserver reports storing, at peak")
	line("%s", strings.Repeat("-", 82))
	for _, resource := range interesting(storageBefore, storagePeak) {
		line("  %-46s %6d -> %6d", resource, storageBefore[resource], storagePeak[resource])
	}

	t.Log(out.String())
}

// interesting picks the resources whose stored count moved, plus the two widget
// resources whether they moved or not -- the offloaded one not moving is the
// result, so it has to be shown.
func interesting(before, after map[string]int) []string {
	names := map[string]bool{}
	for resource, count := range after {
		if before[resource] != count {
			names[resource] = true
		}
	}
	for resource := range before {
		if strings.Contains(resource, "widgets") {
			names[resource] = true
		}
	}
	for resource := range after {
		if strings.Contains(resource, "widgets") {
			names[resource] = true
		}
	}

	var sorted []string
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	return sorted
}

func round(d time.Duration) string {
	switch {
	case d == 0:
		return "-"
	case d < time.Millisecond:
		return d.Round(10 * time.Microsecond).String()
	default:
		return d.Round(100 * time.Microsecond).String()
	}
}
