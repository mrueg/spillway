package kcp

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

// GroupMatcher decides which of a workspace's API groups spillway serves.
//
// An exact name is the whole story for a group that already exists. A pattern
// is for the ones that do not yet: "*.example.com" means every group under that
// domain the workspace serves, now or later, so a CRD in a new group is picked
// up by the same watch that already notices a CRD in a new version -- without
// editing a flag and restarting.
//
// Patterns are deliberately not "everything". Spillway is reached through an
// APIService, and an APIService for a group the cluster serves itself is not a
// mistake you recover from quickly: it puts spillway in front of apps or rbac.
// A pattern therefore has to name at least a two label domain, and may not
// cover Kubernetes' own.
type GroupMatcher struct {
	exact    sets.Set[string]
	suffixes []string

	// excludedExact and excludedSuffixes narrow a wildcard. A blanket is the
	// only reason to need this: naming groups one by one already excludes
	// everything else.
	excludedExact    sets.Set[string]
	excludedSuffixes []string
}

// reservedSuffix is Kubernetes' own domain. A pattern covering it would put
// spillway in front of the cluster's built-in APIs.
const reservedSuffix = ".k8s.io"

// ParseGroupMatcher builds a matcher from what --api-group was given.
func ParseGroupMatcher(patterns []string) (*GroupMatcher, error) {
	matcher := &GroupMatcher{exact: sets.New[string](), excludedExact: sets.New[string]()}

	for _, pattern := range patterns {
		// An exclusion narrows what the wildcards above it take. It is checked
		// first and wins, so the order of the entries does not matter -- a
		// configuration where it did would be a configuration that has to be
		// read in order to be understood.
		if excluded, found := strings.CutPrefix(pattern, "!"); found {
			if err := matcher.exclude(excluded); err != nil {
				return nil, err
			}
			continue
		}

		switch {
		case pattern == "":
			return nil, fmt.Errorf("an API group must not be empty; the core group cannot be offloaded")
		case strings.Contains(pattern, "/"):
			return nil, fmt.Errorf("API group %q must be a group name without a version, e.g. widgets.example.com", pattern)

		case !strings.Contains(pattern, "*"):
			matcher.exact.Insert(pattern)

		case !strings.HasPrefix(pattern, "*."):
			return nil, fmt.Errorf("API group pattern %q must be a full group name or a domain wildcard "+
				"such as *.example.com", pattern)

		default:
			suffix := pattern[1:]
			if strings.Contains(suffix[1:], "*") {
				return nil, fmt.Errorf("API group pattern %q may have only the one leading wildcard", pattern)
			}
			if strings.Count(suffix, ".") < 2 {
				return nil, fmt.Errorf("API group pattern %q is too broad: name at least a two label domain, "+
					"such as *.example.com", pattern)
			}
			if suffix == reservedSuffix || strings.HasSuffix(suffix, reservedSuffix) {
				return nil, fmt.Errorf("API group pattern %q would cover Kubernetes' own API groups, which "+
					"the cluster serves itself", pattern)
			}
			matcher.suffixes = append(matcher.suffixes, suffix)
		}
	}

	if matcher.Empty() {
		return nil, fmt.Errorf("at least one API group is required; spillway would serve nothing otherwise")
	}
	return matcher, nil
}

// exclude records a group, or a domain of them, that must not be served
// whatever the other entries say.
//
// The rules a wildcard has to obey do not apply here. They exist because a
// blanket that is too broad puts spillway in front of an API the cluster is
// using; an exclusion that is too broad only means spillway serves less than
// was meant, which is visible immediately and breaks nothing.
func (m *GroupMatcher) exclude(pattern string) error {
	switch {
	case pattern == "":
		return fmt.Errorf("an excluded API group must not be empty")
	case strings.Contains(pattern, "/"):
		return fmt.Errorf("excluded API group %q must be a group name without a version", pattern)

	case !strings.Contains(pattern, "*"):
		m.excludedExact.Insert(pattern)
	case strings.HasPrefix(pattern, "*.") && !strings.Contains(pattern[2:], "*"):
		m.excludedSuffixes = append(m.excludedSuffixes, pattern[1:])
	default:
		return fmt.Errorf("excluded API group %q must be a full group name or a domain wildcard "+
			"such as !*.internal.example.com", pattern)
	}
	return nil
}

// Matches reports whether spillway serves this group on the workspace's behalf.
func (m *GroupMatcher) Matches(group string) bool {
	if m == nil {
		return false
	}
	if m.excluded(group) {
		return false
	}
	if m.exact.Has(group) {
		return true
	}
	for _, suffix := range m.suffixes {
		if strings.HasSuffix(group, suffix) && len(group) > len(suffix) {
			return true
		}
	}
	return false
}

// excluded reports whether a group has been taken back out.
func (m *GroupMatcher) excluded(group string) bool {
	if m.excludedExact.Has(group) {
		return true
	}
	for _, suffix := range m.excludedSuffixes {
		if strings.HasSuffix(group, suffix) && len(group) > len(suffix) {
			return true
		}
	}
	return false
}

// Empty reports whether the matcher would serve nothing at all.
func (m *GroupMatcher) Empty() bool {
	return m == nil || (m.exact.Len() == 0 && len(m.suffixes) == 0)
}

// Exact returns the groups named outright, sorted. These are the ones whose
// absence is a fault worth reporting: somebody said this group should be here.
// A pattern matching nothing is not a fault -- it is a workspace that has not
// been given that kind of CRD yet.
func (m *GroupMatcher) Exact() []string {
	if m == nil {
		return nil
	}
	// A named group that is also excluded is not something to report the
	// absence of: the configuration says both, and the exclusion wins.
	named := make([]string, 0, m.exact.Len())
	for _, group := range sets.List(m.exact) {
		if !m.excluded(group) {
			named = append(named, group)
		}
	}
	return named
}

// String renders the matcher the way it was configured, for logs and errors.
func (m *GroupMatcher) String() string {
	if m == nil {
		return ""
	}
	parts := sets.List(m.exact)
	for _, suffix := range m.suffixes {
		parts = append(parts, "*"+suffix)
	}
	for _, group := range sets.List(m.excludedExact) {
		parts = append(parts, "!"+group)
	}
	for _, suffix := range m.excludedSuffixes {
		parts = append(parts, "!*"+suffix)
	}
	return strings.Join(parts, ",")
}
