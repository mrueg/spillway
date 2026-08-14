package kcp

import "testing"

func TestGroupMatcherMatches(t *testing.T) {
	matcher, err := ParseGroupMatcher([]string{"widgets.example.com", "*.tenant.example.net"})
	if err != nil {
		t.Fatalf("ParseGroupMatcher: %v", err)
	}

	for group, want := range map[string]bool{
		"widgets.example.com":        true,
		"gadgets.tenant.example.net": true,
		"a.b.tenant.example.net":     true,

		// The domain itself is not a group under it.
		"tenant.example.net": false,
		// A suffix match must be on a label boundary, or "evil-example.net"
		// would match "*.example.net".
		"eviltenant.example.net": false,
		"gadgets.example.com":    false,
		"apps":                   false,
		"":                       false,
	} {
		if got := matcher.Matches(group); got != want {
			t.Errorf("Matches(%q) = %v, want %v", group, got, want)
		}
	}
}

// A pattern is a blanket, and the blanket must not cover the cluster's own
// APIs: an APIService in front of apps or rbac is not a quick recovery.
func TestGroupMatcherRefusesDangerousPatterns(t *testing.T) {
	for _, pattern := range []string{
		"*",        // everything
		"*.io",     // one label: too broad to be meant
		"*.k8s.io", // Kubernetes' own
		"*.authorization.k8s.io",
		"*example.com",     // not a label boundary
		"wid*.example.com", // wildcard in the middle
		"*.*.example.com",  // two wildcards
		"widgets.example.com/v1",
		"",
	} {
		if _, err := ParseGroupMatcher([]string{pattern}); err == nil {
			t.Errorf("ParseGroupMatcher(%q) was accepted, want it refused", pattern)
		}
	}
}

func TestGroupMatcherAcceptsWhatItShould(t *testing.T) {
	for _, pattern := range []string{"widgets.example.com", "*.example.com", "*.a.b.c"} {
		if _, err := ParseGroupMatcher([]string{pattern}); err != nil {
			t.Errorf("ParseGroupMatcher(%q): %v", pattern, err)
		}
	}
}

// An exact name is something somebody asserted should exist, so its absence is
// reportable. A pattern matching nothing is just a workspace without that kind
// of CRD yet, so it is not.
func TestGroupMatcherExactExcludesPatterns(t *testing.T) {
	matcher, err := ParseGroupMatcher([]string{"widgets.example.com", "*.tenant.example.net"})
	if err != nil {
		t.Fatalf("ParseGroupMatcher: %v", err)
	}

	exact := matcher.Exact()
	if len(exact) != 1 || exact[0] != "widgets.example.com" {
		t.Errorf("Exact() = %v, want just the named group", exact)
	}
	if matcher.String() != "widgets.example.com,*.tenant.example.net" {
		t.Errorf("String() = %q", matcher.String())
	}
}

func TestGroupMatcherRequiresSomething(t *testing.T) {
	if _, err := ParseGroupMatcher(nil); err == nil {
		t.Error("an empty configuration was accepted; spillway would serve nothing")
	}
}

// A wildcard is a blanket, and there is usually one group under the domain that
// should stay in the cluster.
func TestGroupMatcherExcludes(t *testing.T) {
	matcher, err := ParseGroupMatcher([]string{"*.example.com", "!internal.example.com", "!*.private.example.com"})
	if err != nil {
		t.Fatalf("ParseGroupMatcher: %v", err)
	}

	for group, want := range map[string]bool{
		"widgets.example.com": true,
		"gadgets.example.com": true,

		"internal.example.com":         false,
		"anything.private.example.com": false,

		// The excluded domain itself is not under it, so the wildcard above
		// still takes it -- the exclusion says "*.private", not "private".
		"private.example.com": true,
	} {
		if got := matcher.Matches(group); got != want {
			t.Errorf("Matches(%q) = %v, want %v", group, got, want)
		}
	}
}

// An exclusion wins wherever it appears, so a configuration does not have to be
// read in order.
func TestGroupMatcherExclusionWinsOverAnExactName(t *testing.T) {
	for _, order := range [][]string{
		{"widgets.example.com", "!widgets.example.com", "*.other.example.net"},
		{"!widgets.example.com", "widgets.example.com", "*.other.example.net"},
	} {
		matcher, err := ParseGroupMatcher(order)
		if err != nil {
			t.Fatalf("ParseGroupMatcher(%v): %v", order, err)
		}
		if matcher.Matches("widgets.example.com") {
			t.Errorf("%v: the excluded group is served", order)
		}
		// And it is not reported as a group whose absence is a fault.
		for _, named := range matcher.Exact() {
			if named == "widgets.example.com" {
				t.Errorf("%v: the excluded group is still expected to be served", order)
			}
		}
	}
}

// The rules a wildcard obeys do not apply to an exclusion: serving less than
// was meant is visible immediately, where serving more is not.
func TestGroupMatcherAcceptsNarrowExclusions(t *testing.T) {
	for _, pattern := range []string{"!*.k8s.io", "!*.io", "!apps"} {
		if _, err := ParseGroupMatcher([]string{"*.example.com", pattern}); err != nil {
			t.Errorf("ParseGroupMatcher(%q): %v", pattern, err)
		}
	}
	for _, pattern := range []string{"!", "!widgets.example.com/v1", "!wid*.example.com"} {
		if _, err := ParseGroupMatcher([]string{"*.example.com", pattern}); err == nil {
			t.Errorf("ParseGroupMatcher(%q) was accepted", pattern)
		}
	}
}

// Excluding everything that was included leaves nothing, which is a
// configuration mistake rather than a server that serves nothing.
func TestGroupMatcherExclusionsAloneAreRefused(t *testing.T) {
	if _, err := ParseGroupMatcher([]string{"!widgets.example.com"}); err == nil {
		t.Error("a configuration of nothing but exclusions was accepted")
	}
}
