package apiserver

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
)

// clusterAPIs answers which group versions the workload cluster already serves
// by means that are not spillway.
//
// This is a guard on registration, and it exists because of wildcards. An
// APIService takes precedence over the delegate behind it, so registering one
// for a group the cluster serves from its own CustomResourceDefinitions puts
// spillway in front of them: the objects stay in the cluster's etcd and stop
// being reachable through its API. An exact --api-group is a decision somebody
// made about one group. A wildcard is a blanket, and a blanket can cover a group
// the cluster is already using without anyone intending it.
//
// Detection cannot be discovery alone, because a group spillway serves appears
// in the cluster's discovery too -- through spillway's own APIService. So the
// two are read together: a group version the cluster serves is foreign unless
// the APIService routing it points at spillway.
type clusterAPIs struct {
	discovery  discovery.DiscoveryInterface
	aggregator aggregatorclient.Interface

	// service is spillway's own, which is what makes an APIService ours.
	service apiregistrationv1.ServiceReference
}

// foreign returns the group versions the cluster serves without spillway.
//
// An error is not an empty answer. The caller must treat a failure as "do not
// register": registering wrongly hides another API, which is silent and severe,
// while not registering leaves a group unserved, which is loud and recoverable.
func (c *clusterAPIs) foreign(ctx context.Context) (sets.Set[schema.GroupVersion], error) {
	if c == nil {
		return nil, nil
	}

	groups, err := c.discovery.ServerGroups()
	if err != nil {
		return nil, fmt.Errorf("listing the cluster's own API groups: %w", err)
	}

	services, err := c.aggregator.ApiregistrationV1().APIServices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing APIServices: %w", err)
	}
	routing := make(map[string]*apiregistrationv1.APIService, len(services.Items))
	for i := range services.Items {
		routing[services.Items[i].Name] = &services.Items[i]
	}

	foreign := sets.New[schema.GroupVersion]()
	for _, group := range groups.Groups {
		for _, version := range group.Versions {
			gv := schema.GroupVersion{Group: group.Name, Version: version.Version}
			if c.ours(routing[gv.Version+"."+gv.Group]) {
				continue
			}
			foreign.Insert(gv)
		}
	}
	return foreign, nil
}

// ours reports whether an APIService routes to spillway. A nil one is not ours:
// the cluster is serving that group version itself, from a built-in API or a
// CustomResourceDefinition.
func (c *clusterAPIs) ours(service *apiregistrationv1.APIService) bool {
	if service == nil || service.Spec.Service == nil {
		return false
	}
	return service.Spec.Service.Name == c.service.Name &&
		service.Spec.Service.Namespace == c.service.Namespace
}
