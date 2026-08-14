package apiserver

import (
	"net/http"
	"strings"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/klog/v2"

	"github.com/mrueg/spillway/pkg/kcp"
)

// discoveryHandler serves the discovery endpoints for the groups spillway owns,
// answering from the kcp resource cache.
//
// The group versions of a workspace are not known until the cache first syncs,
// and they can change afterwards, so the handlers are registered per group as a
// prefix and resolve the version per request rather than being installed with
// InstallAPIGroup.
type discoveryHandler struct {
	cache snapshotter
	// owns reports whether a group is configured to be spillway's, which a
	// wildcard makes a question rather than a list.
	owns func(group string) bool

	// published is what was last put into the discovery documents, so a group
	// that disappears from the workspace can be withdrawn from them.
	published sets.Set[string]

	// discovery and aggregated are the server's two discovery documents. They
	// are interfaces so publish can be exercised without a running server;
	// aggregated is nil when the server has aggregated discovery disabled.
	discovery  groupManager
	aggregated aggregatedManager

	// proxy resolves the handler that forwards a group's resource requests to
	// the workspace backing it. When it returns nil, they are refused as not
	// implemented rather than silently returning nothing.
	proxy func(group string) http.Handler
}

// snapshotter is the read side of the kcp resource cache, kept as an interface
// so the handlers can be exercised without a live workspace.
type snapshotter interface {
	Snapshot() *kcp.Snapshot
}

// groupManager is the part of the server's /apis document that publish writes.
type groupManager interface {
	AddGroup(apiGroup metav1.APIGroup)
	RemoveGroup(groupName string)
}

// aggregatedManager is the same for aggregated discovery.
type aggregatedManager interface {
	AddGroupVersion(groupName string, value apidiscoveryv2.APIVersionDiscovery)
	SetGroupVersionPriority(gv metav1.GroupVersion, grouppriority, versionpriority int)
	RemoveGroup(groupName string)
}

// Ordering within spillway's own aggregated discovery document. Where these
// groups sit relative to the rest of the cluster's APIs is decided by the
// APIService, not here; setting a priority at all is what keeps spillway's
// answer stable instead of ordered by map iteration.
const (
	discoveryGroupPriority   = 1000
	discoveryVersionPriority = 15
)

// install registers the discovery endpoints.
//
// One prefix handler rather than one per group: with a wildcard, the groups are
// not known when the server is built, and a mux cannot have a path registered
// onto it twice. The group is resolved per request instead.
//
// Only the exact paths /apis and /apis/ are dispatched to go-restful, and exact
// registrations win over prefixes, so the server's own discovery documents are
// untouched by this.
func (h *discoveryHandler) install(mux *mux.PathRecorderMux) {
	mux.HandlePrefix("/apis/", http.HandlerFunc(h.serve))
}

// serve resolves the group from the path and dispatches on what follows it.
func (h *discoveryHandler) serve(w http.ResponseWriter, req *http.Request) {
	remainder := strings.TrimPrefix(req.URL.Path, "/apis/")
	group, _, hasRest := strings.Cut(remainder, "/")

	if group == "" || (h.owns != nil && !h.owns(group)) {
		h.writeGroupNotFound(group, w, req)
		return
	}
	if !hasRest {
		h.serveGroup(group, w, req)
		return
	}
	h.serveUnderGroup(group, w, req)
}

// serveGroup answers /apis/<group> with the APIGroup kcp reported.
func (h *discoveryHandler) serveGroup(group string, w http.ResponseWriter, req *http.Request) {
	apiGroup, found := h.cache.Snapshot().Groups[group]
	if !found {
		h.writeGroupNotFound(group, w, req)
		return
	}

	discovery.NewAPIGroupHandler(Codecs, apiGroup).ServeHTTP(w, req)
}

// serveUnderGroup answers /apis/<group>/<version> with the resources of that
// version. Anything deeper is a request for a resource, which spillway does not
// proxy yet.
func (h *discoveryHandler) serveUnderGroup(group string, w http.ResponseWriter, req *http.Request) {
	remainder := strings.TrimPrefix(req.URL.Path, "/apis/"+group+"/")
	version, rest, _ := strings.Cut(remainder, "/")

	if version == "" {
		h.serveGroup(group, w, req)
		return
	}

	gv := schema.GroupVersion{Group: group, Version: version}
	resources, found := h.cache.Snapshot().Resources[gv]
	if !found {
		h.writeGroupNotFound(group, w, req)
		return
	}

	if rest != "" {
		forward := h.proxyTo(group)
		if forward == nil {
			responsewriters.ErrorNegotiated(
				apierrors.NewGenericServerResponse(
					http.StatusNotImplemented, req.Method,
					schema.GroupResource{Group: group, Resource: resourceFromPath(rest)}, "",
					"spillway is not configured to proxy resource requests to kcp",
					0, false,
				),
				Codecs, gv, w, req)
			return
		}
		forward.ServeHTTP(w, req)
		return
	}

	lister := discovery.APIResourceListerFunc(func() []metav1.APIResource { return resources })
	discovery.NewAPIVersionHandler(Codecs, gv, lister).ServeHTTP(w, req)
}

// proxyTo resolves the handler for a group, if there is one.
func (h *discoveryHandler) proxyTo(group string) http.Handler {
	if h.proxy == nil {
		return nil
	}
	return h.proxy(group)
}

// resourceFromPath picks the resource out of the part of a request path that
// follows the group version, so that errors name the resource the client asked
// for rather than the "namespaces" segment of a namespaced path.
func resourceFromPath(path string) string {
	segments := strings.Split(path, "/")
	if segments[0] == "namespaces" && len(segments) >= 3 {
		return segments[2]
	}
	return segments[0]
}

func (h *discoveryHandler) writeGroupNotFound(group string, w http.ResponseWriter, req *http.Request) {
	responsewriters.ErrorNegotiated(
		apierrors.NewNotFound(schema.GroupResource{Group: group, Resource: "apigroup"}, group),
		Codecs, schema.GroupVersion{Group: group}, w, req)
}

// publish pushes the current snapshot into the server's discovery managers, so
// that /apis lists the owned groups and aggregated discovery reports their
// resources. It runs after every successful cache refresh.
func (h *discoveryHandler) publish() {
	snapshot := h.cache.Snapshot()

	// Anything published before and gone now is withdrawn, rather than left
	// pointing clients at resources that no longer exist.
	current := sets.New[string]()
	for group := range snapshot.Groups {
		current.Insert(group)
	}
	for _, group := range h.published.Difference(current).UnsortedList() {
		h.discovery.RemoveGroup(group)
		if h.aggregated != nil {
			h.aggregated.RemoveGroup(group)
		}
	}
	h.published = current

	for group := range snapshot.Groups {
		apiGroup := snapshot.Groups[group]
		h.discovery.AddGroup(apiGroup)

		if h.aggregated == nil {
			continue
		}
		for _, version := range apiGroup.Versions {
			gv, err := schema.ParseGroupVersion(version.GroupVersion)
			if err != nil {
				klog.Background().Error(err, "Skipping malformed group version from kcp", "groupVersion", version.GroupVersion)
				continue
			}
			h.aggregated.AddGroupVersion(group, aggregatedVersion(gv, snapshot.Resources[gv]))
			h.aggregated.SetGroupVersionPriority(
				metav1.GroupVersion{Group: gv.Group, Version: gv.Version},
				discoveryGroupPriority, discoveryVersionPriority)
		}
	}
}

// aggregatedVersion converts a discovery APIResourceList into the aggregated
// discovery form, which nests subresources under their parent rather than
// listing them as "widgets/status" alongside it.
func aggregatedVersion(gv schema.GroupVersion, resources []metav1.APIResource) apidiscoveryv2.APIVersionDiscovery {
	parents := map[string]*apidiscoveryv2.APIResourceDiscovery{}
	subresources := map[string][]apidiscoveryv2.APISubresourceDiscovery{}
	var order []string

	for _, resource := range resources {
		name, subresource, isSubresource := strings.Cut(resource.Name, "/")

		if isSubresource {
			subresources[name] = append(subresources[name], apidiscoveryv2.APISubresourceDiscovery{
				Subresource:  subresource,
				ResponseKind: responseKind(gv, resource),
				Verbs:        resource.Verbs,
			})
			continue
		}

		scope := apidiscoveryv2.ScopeCluster
		if resource.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		}

		parents[name] = &apidiscoveryv2.APIResourceDiscovery{
			Resource:         name,
			ResponseKind:     responseKind(gv, resource),
			Scope:            scope,
			SingularResource: resource.SingularName,
			Verbs:            resource.Verbs,
			ShortNames:       resource.ShortNames,
			Categories:       resource.Categories,
		}
		order = append(order, name)
	}

	discovered := make([]apidiscoveryv2.APIResourceDiscovery, 0, len(order))
	for _, name := range order {
		parent := parents[name]
		parent.Subresources = subresources[name]
		discovered = append(discovered, *parent)
	}

	return apidiscoveryv2.APIVersionDiscovery{
		Version:   gv.Version,
		Resources: discovered,
		Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
	}
}

// responseKind resolves the kind a resource returns. Discovery may leave the
// group and version empty on an entry, meaning the same as the list it came in.
func responseKind(gv schema.GroupVersion, resource metav1.APIResource) *metav1.GroupVersionKind {
	kind := metav1.GroupVersionKind{
		Group:   resource.Group,
		Version: resource.Version,
		Kind:    resource.Kind,
	}
	if kind.Group == "" && kind.Version == "" {
		kind.Group, kind.Version = gv.Group, gv.Version
	}
	return &kind
}
