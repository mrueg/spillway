package apiserver

import (
	"testing"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestAggregatedVersionNestsSubresources(t *testing.T) {
	gv := schema.GroupVersion{Group: "spillway.example.com", Version: "v1alpha1"}

	// Shaped like what a discovery client returns for a CRD with a status
	// subresource: the subresource is a sibling entry, not a nested one.
	got := aggregatedVersion(gv, []metav1.APIResource{
		{
			Name:         "widgets",
			SingularName: "widget",
			Namespaced:   true,
			Kind:         "Widget",
			Verbs:        metav1.Verbs{"get", "list", "watch", "create", "update", "delete"},
			ShortNames:   []string{"wdg"},
		},
		{
			Name:       "widgets/status",
			Namespaced: true,
			Kind:       "Widget",
			Verbs:      metav1.Verbs{"get", "patch", "update"},
		},
	})

	if got.Version != "v1alpha1" {
		t.Errorf("Version = %q, want %q", got.Version, "v1alpha1")
	}
	if got.Freshness != apidiscoveryv2.DiscoveryFreshnessCurrent {
		t.Errorf("Freshness = %q, want %q", got.Freshness, apidiscoveryv2.DiscoveryFreshnessCurrent)
	}

	if len(got.Resources) != 1 {
		t.Fatalf("got %d top level resources, want 1 (the subresource must not be listed alongside its parent): %+v",
			len(got.Resources), got.Resources)
	}

	widgets := got.Resources[0]
	if widgets.Resource != "widgets" {
		t.Errorf("Resource = %q, want %q", widgets.Resource, "widgets")
	}
	if widgets.Scope != apidiscoveryv2.ScopeNamespace {
		t.Errorf("Scope = %q, want %q", widgets.Scope, apidiscoveryv2.ScopeNamespace)
	}
	if widgets.SingularResource != "widget" {
		t.Errorf("SingularResource = %q, want %q", widgets.SingularResource, "widget")
	}

	// Discovery leaves group and version empty on entries that match the list
	// they arrived in; aggregated discovery requires them to be filled in.
	want := &metav1.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: "Widget"}
	if widgets.ResponseKind == nil || *widgets.ResponseKind != *want {
		t.Errorf("ResponseKind = %+v, want %+v", widgets.ResponseKind, want)
	}

	if len(widgets.Subresources) != 1 {
		t.Fatalf("got %d subresources, want 1: %+v", len(widgets.Subresources), widgets.Subresources)
	}
	if widgets.Subresources[0].Subresource != "status" {
		t.Errorf("Subresource = %q, want %q", widgets.Subresources[0].Subresource, "status")
	}
}

func TestResourceFromPath(t *testing.T) {
	for path, want := range map[string]string{
		"widgets":                               "widgets",
		"widgets/red-widget":                    "widgets",
		"namespaces/default/widgets":            "widgets",
		"namespaces/default/widgets/red":        "widgets",
		"namespaces/default/widgets/red/status": "widgets",
		"namespaces":                            "namespaces",
	} {
		if got := resourceFromPath(path); got != want {
			t.Errorf("resourceFromPath(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestAggregatedVersionScopesClusterResources(t *testing.T) {
	got := aggregatedVersion(
		schema.GroupVersion{Group: "spillway.example.com", Version: "v1"},
		[]metav1.APIResource{{Name: "clusterwidgets", Kind: "ClusterWidget", Namespaced: false}},
	)

	if len(got.Resources) != 1 {
		t.Fatalf("got %d resources, want 1", len(got.Resources))
	}
	if got.Resources[0].Scope != apidiscoveryv2.ScopeCluster {
		t.Errorf("Scope = %q, want %q", got.Resources[0].Scope, apidiscoveryv2.ScopeCluster)
	}
}

// A group version with no resources still has to produce a valid entry: an
// empty workspace must not make aggregated discovery malformed.
func TestAggregatedVersionWithNoResources(t *testing.T) {
	got := aggregatedVersion(schema.GroupVersion{Group: "spillway.example.com", Version: "v1"}, nil)

	if got.Version != "v1" {
		t.Errorf("Version = %q, want %q", got.Version, "v1")
	}
	if len(got.Resources) != 0 {
		t.Errorf("got %d resources, want 0", len(got.Resources))
	}
}
