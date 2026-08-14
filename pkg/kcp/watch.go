package kcp

import (
	"context"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// WatchCRDs reports when the API surface of the workspace may have changed, by
// watching the CustomResourceDefinitions of the owned groups.
//
// The signal is only a hint that something moved. What spillway serves is still
// decided by re-reading discovery, because that is the workspace's own answer to
// the question and it accounts for a CRD that exists but is not yet Established.
//
// The returned channel has room for one pending signal, so a burst -- applying
// a bundle of CRDs at once, or an informer relist -- collapses into a single
// refresh instead of one per object.
func WatchCRDs(ctx context.Context, config *rest.Config, owned *GroupMatcher) (<-chan struct{}, error) {
	client, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building an apiextensions client for kcp: %w", err)
	}

	changed, err := watchCRDs(ctx, client, owned)
	if err != nil {
		return nil, err
	}

	// A CRD is not the only way a group arrives. kcp shares APIs between
	// workspaces with APIBindings, and a bound resource has no
	// CustomResourceDefinition in the workspace that binds it -- checked
	// against a real kcp, where a workspace serving a bound group listed no CRD
	// for it. Watching only CRDs therefore misses it entirely, and the group
	// appears whenever the backstop refresh next runs: ten minutes by default,
	// for something the operator did seconds ago.
	if err := watchBindings(ctx, config, changed); err != nil {
		// Not fatal. Without this, spillway is as prompt as it was before, and
		// a workspace that does not serve APIBindings -- or an identity not
		// permitted to watch them -- should not stop it serving.
		klog.FromContext(ctx).V(2).Info("Not watching APIBindings; bound APIs will appear on the backstop refresh instead",
			"err", err)
	}

	return changed, nil
}

// bindingsResource is where kcp keeps the bindings a workspace has made.
var bindingsResource = schema.GroupVersionResource{
	Group: "apis.kcp.io", Version: "v1alpha2", Resource: "apibindings",
}

// watchBindings signals on any APIBinding event.
//
// Any, rather than the ones whose groups are owned: what a binding brings in is
// in its status once it is bound, so filtering on it would mean waiting for the
// thing the signal exists to notice. Bindings are rare and a refresh is cheap
// and coalesced, so the untargeted signal costs less than the machinery to aim
// it would.
func watchBindings(ctx context.Context, config *rest.Config, changed chan<- struct{}) error {
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("building a dynamic client for kcp: %w", err)
	}

	// Asked for before the informer is started, so a workspace without the type
	// is a log line rather than an informer retrying forever.
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("building a discovery client for kcp: %w", err)
	}
	resources, err := discoveryClient.ServerResourcesForGroupVersion(bindingsResource.GroupVersion().String())
	if err != nil {
		return fmt.Errorf("looking for APIBindings in the workspace: %w", err)
	}
	found := false
	for _, resource := range resources.APIResources {
		if resource.Name == bindingsResource.Resource {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("the workspace does not serve %s", bindingsResource)
	}

	notify := func(any) {
		select {
		case changed <- struct{}{}:
		default: // a refresh is already pending; it will see this too
		}
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, 0, metav1.NamespaceAll, nil)
	informer := factory.ForResource(bindingsResource).Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    notify,
		UpdateFunc: func(_, obj any) { notify(obj) },
		DeleteFunc: notify,
	}); err != nil {
		return fmt.Errorf("watching APIBindings: %w", err)
	}

	go informer.Run(ctx.Done())
	return nil
}

// watchCRDs is WatchCRDs with the client supplied, so the filtering and
// coalescing can be exercised without an API server.
func watchCRDs(ctx context.Context, client apiextensionsclient.Interface, owned *GroupMatcher) (chan struct{}, error) {
	changed := make(chan struct{}, 1)

	notify := func(obj any) {
		crd, ok := crdFrom(obj)
		if !ok || !owned.Matches(crd.Spec.Group) {
			return
		}
		select {
		case changed <- struct{}{}:
		default: // a refresh is already pending; it will see this change too
		}
	}

	// No informer resync: the informer relists on its own when the watch
	// breaks, and ResourceCache.Run keeps a periodic refresh as the backstop
	// for anything a watch cannot tell us about.
	factory := apiextensionsinformers.NewSharedInformerFactory(client, 0)
	informer := factory.Apiextensions().V1().CustomResourceDefinitions().Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    notify,
		UpdateFunc: func(_, obj any) { notify(obj) },
		DeleteFunc: notify,
	}); err != nil {
		return nil, fmt.Errorf("watching CustomResourceDefinitions in kcp: %w", err)
	}

	factory.Start(ctx.Done())

	return changed, nil
}

// crdFrom unwraps an informer event, which delivers a deletion as a tombstone
// when the watch missed the delete itself.
func crdFrom(obj any) (*apiextensionsv1.CustomResourceDefinition, bool) {
	switch typed := obj.(type) {
	case *apiextensionsv1.CustomResourceDefinition:
		return typed, true
	case cache.DeletedFinalStateUnknown:
		crd, ok := typed.Obj.(*apiextensionsv1.CustomResourceDefinition)
		return crd, ok
	default:
		return nil, false
	}
}
