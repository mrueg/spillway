package apiserver

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorfake "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/fake"
	"k8s.io/utils/ptr"
)

// clusterDiscovery reports the groups a workload cluster serves.
type clusterDiscovery struct {
	discovery.DiscoveryInterface

	groups []string
	// version the groups are served at, defaulting to v1.
	version string
	err     error
}

func (d *clusterDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	if d.err != nil {
		return nil, d.err
	}
	version := d.version
	if version == "" {
		version = "v1"
	}
	list := &metav1.APIGroupList{}
	for _, group := range d.groups {
		list.Groups = append(list.Groups, metav1.APIGroup{
			Name:     group,
			Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: group + "/" + version, Version: version}},
		})
	}
	return list, nil
}

func spillwayService() apiregistrationv1.ServiceReference {
	return apiregistrationv1.ServiceReference{Name: "spillway", Namespace: "spillway-system"}
}

func routedBy(name, group string, service *apiregistrationv1.ServiceReference) runtime.Object {
	return &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiregistrationv1.APIServiceSpec{
			Group: group, Version: "v1", Service: service,
		},
	}
}

// A group the cluster serves itself has an APIService with no service on it --
// that is what "Local" means, and it covers both the built-in APIs and every
// CustomResourceDefinition, since kube-apiserver registers one per CRD group.
func TestForeignFindsWhatTheClusterServesItself(t *testing.T) {
	cluster := &clusterAPIs{
		discovery: &clusterDiscovery{groups: []string{"apps", "widgets.example.com", "metrics.k8s.io"}},
		aggregator: aggregatorfake.NewSimpleClientset(
			routedBy("v1.apps", "apps", nil),
			routedBy("v1.widgets.example.com", "widgets.example.com", nil),
			routedBy("v1.metrics.k8s.io", "metrics.k8s.io",
				&apiregistrationv1.ServiceReference{Name: "metrics-server", Namespace: "kube-system", Port: ptr.To(int32(443))}),
		),
		service: spillwayService(),
	}

	foreign, err := cluster.foreign(context.Background())
	if err != nil {
		t.Fatalf("foreign: %v", err)
	}

	for _, group := range []string{"apps", "widgets.example.com"} {
		if !foreign.Has(schema.GroupVersion{Group: group, Version: "v1"}) {
			t.Errorf("%s is not reported as the cluster's own; spillway would register over it", group)
		}
	}
	// Another component's aggregated API is the worse case: nothing reconciles
	// that APIService back, so registering over it redirects it for good.
	if !foreign.Has(schema.GroupVersion{Group: "metrics.k8s.io", Version: "v1"}) {
		t.Error("another component's aggregated API is not reported as foreign")
	}
}

// Spillway's own groups appear in the cluster's discovery too, through
// spillway's APIService. Reading them as foreign would make it withdraw from
// everything it serves.
func TestForeignExcludesSpillwaysOwn(t *testing.T) {
	service := spillwayService()
	cluster := &clusterAPIs{
		discovery: &clusterDiscovery{groups: []string{"widgets.example.com"}},
		aggregator: aggregatorfake.NewSimpleClientset(
			routedBy("v1.widgets.example.com", "widgets.example.com", &service)),
		service: service,
	}

	foreign, err := cluster.foreign(context.Background())
	if err != nil {
		t.Fatalf("foreign: %v", err)
	}
	if foreign.Has(schema.GroupVersion{Group: "widgets.example.com", Version: "v1"}) {
		t.Error("spillway's own group was reported as foreign")
	}
}

// Not knowing is not permission. A failure has to stop the round rather than
// read as "the cluster serves nothing".
func TestForeignFailsRatherThanReturningNothing(t *testing.T) {
	cluster := &clusterAPIs{
		discovery:  &clusterDiscovery{err: errors.New("apiserver unreachable")},
		aggregator: aggregatorfake.NewSimpleClientset(),
		service:    spillwayService(),
	}

	if _, err := cluster.foreign(context.Background()); err == nil {
		t.Error("a discovery failure was reported as an empty answer, which would let spillway register over anything")
	}
}
