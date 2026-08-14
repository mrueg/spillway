// Package version reports the build version of spillway.
package version

import (
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// fallback is what a build without the link time flag reports to the
// aggregation layer, which needs something parseable.
const fallback = "0.0.0"

// Version is the build version. It is set at link time and nowhere else:
//
//	-X github.com/mrueg/spillway/pkg/version.Version=<version>
//
// goreleaser supplies it for releases and snapshots; "make build" supplies it
// from git. A build with neither reports "dev".
var Version = "dev"

// APIVersion returns Version in a form the aggregation layer's compatibility
// machinery can parse. Development builds report "dev", which is not a version,
// so they fall back rather than failing at startup.
func APIVersion() string {
	parsed, err := utilversion.ParseSemantic(Version)
	if err != nil {
		return fallback
	}
	return parsed.String()
}
