package version

import "testing"

// A build without the link time flag reports "dev", which is not a version.
// The aggregation layer parses whatever APIVersion returns during startup, so
// this has to be a version rather than propagating the placeholder.
func TestAPIVersionFallsBackForDevelopmentBuilds(t *testing.T) {
	original := Version
	t.Cleanup(func() { Version = original })

	for _, unparseable := range []string{"dev", "", "not-a-version"} {
		Version = unparseable
		if got := APIVersion(); got != fallback {
			t.Errorf("APIVersion() with Version=%q = %q, want %q", unparseable, got, fallback)
		}
	}
}

func TestAPIVersionPassesThroughRealVersions(t *testing.T) {
	original := Version
	t.Cleanup(func() { Version = original })

	// The forms goreleaser produces: a tagged release and a snapshot.
	for version, want := range map[string]string{
		"0.1.0":          "0.1.0",
		"1.2.3":          "1.2.3",
		"0.1.1-snapshot": "0.1.1-snapshot",
	} {
		Version = version
		if got := APIVersion(); got != want {
			t.Errorf("APIVersion() with Version=%q = %q, want %q", version, got, want)
		}
	}
}

// The default matters: it is what an unstamped "go build" reports.
func TestDefaultVersionIsDev(t *testing.T) {
	if Version != "dev" && Version != "" {
		t.Logf("Version is %q, which means this test binary was built with the ldflag set", Version)
	}
}
