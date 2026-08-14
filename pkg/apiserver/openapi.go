package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/aggregator"
	"k8s.io/kube-openapi/pkg/handler3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// specFetchTimeout bounds a single fetch from kcp. OpenAPI is not on the
// aggregation layer's availability path, so a slow answer here is survivable,
// but it must not pin a connection open indefinitely.
const specFetchTimeout = 10 * time.Second

// maxSpecBytes bounds a single OpenAPI document read into memory.
const maxSpecBytes = 32 << 20

// specFetcher retrieves a raw document from kcp by absolute path.
type specFetcher interface {
	fetchSpec(ctx context.Context, path string) ([]byte, error)
}

// openAPIHandler serves OpenAPI v3 for the offloaded groups by proxying the
// workspace's own documents.
//
// The schemas belong to kcp, which already publishes them for the CRDs it
// serves, so they are forwarded rather than rebuilt. v3 makes this practical:
// it is published per group version, so spillway can expose exactly the groups
// it owns. The v2 document is a single merged blob covering the whole server,
// and forwarding it would inject every API kcp serves -- including kcp's own
// tenancy and apis groups -- into the workload cluster's merged spec.
type openAPIHandler struct {
	cache  snapshotter
	groups func() sets.Set[string]

	// fetcher is the side channel to the workspace backing a group, for the v3
	// document that describes only that group.
	fetcher func(group string) specFetcher
	// fetchers is every workspace's, for the v2 document, which is one blob
	// describing everything spillway serves.
	fetchers func() []namedFetcher

	// generation reports the API surface the documents should describe, so a
	// cached one can be recognised as out of date.
	generation func() uint64

	index *documentCache // /openapi/v3
	v2    *documentCache // /openapi/v2
}

// prepare builds the document caches. Both documents are expensive enough that
// rebuilding them per request is the difference between a poll the aggregation
// layer does forever and one it barely notices.
func (h *openAPIHandler) prepare() {
	h.index = newDocumentCache(h.buildIndex)
	h.v2 = newDocumentCache(h.buildV2)
}

// currentGeneration is 0 when nothing reports one, which makes the cache
// permanent rather than per request -- the right behaviour for a handler
// assembled without a resource cache, as in tests.
func (h *openAPIHandler) currentGeneration() uint64 {
	if h.generation == nil {
		return 0
	}
	return h.generation()
}

func (h *openAPIHandler) install(pathMux *mux.PathRecorderMux) {
	pathMux.HandleFunc("/openapi/v2", h.serveV2)
	pathMux.HandleFunc("/openapi/v3", h.serveDiscovery)
	pathMux.HandlePrefix("/openapi/v3/", http.HandlerFunc(h.serveSpec))
}

// serveDiscovery answers /openapi/v3 with the group versions spillway owns,
// filtered out of the workspace's own index.
func (h *openAPIHandler) serveDiscovery(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), specFetchTimeout)
	defer cancel()

	document, err := h.index.get(ctx, h.currentGeneration())
	if err != nil {
		h.writeUnavailable(err, w, req)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(document)
}

// buildIndex fetches kcp's own v3 index and keeps the group versions spillway
// owns.
func (h *openAPIHandler) buildIndex(ctx context.Context) ([]byte, error) {
	filtered := handler3.OpenAPIV3Discovery{Paths: map[string]handler3.OpenAPIV3DiscoveryGroupVersion{}}

	// Every workspace publishes an index of its own. What spillway serves is
	// the union, filtered to the groups it owns -- so a group version appears
	// exactly once, from the workspace that backs it.
	for _, source := range h.sources() {
		raw, err := source.fetcher.fetchSpec(ctx, "/openapi/v3")
		if err != nil {
			return nil, fmt.Errorf("fetching the OpenAPI v3 index from workspace %s: %w", source.name, err)
		}

		var index handler3.OpenAPIV3Discovery
		if err := json.Unmarshal(raw, &index); err != nil {
			return nil, fmt.Errorf("parsing the OpenAPI v3 index from workspace %s: %w", source.name, err)
		}

		for path, gv := range index.Paths {
			if h.owns(path) {
				// kcp's relative URL is valid against spillway too: the same path
				// is served here, and keeping the hash preserves cache busting.
				filtered.Paths[path] = gv
			}
		}
	}

	return json.Marshal(filtered)
}

// serveSpec answers /openapi/v3/apis/<group>/<version> from the workspace.
func (h *openAPIHandler) serveSpec(w http.ResponseWriter, req *http.Request) {
	// owns only inspects the first three segments, so without this a path like
	// apis/<owned group>/<version>/../../../../api/v1 would pass the ownership
	// check and then be fetched from kcp with spillway's credentials.
	if !canonicalPath(req.URL.Path) {
		responsewriters.ErrorNegotiated(
			apierrors.NewBadRequest(fmt.Sprintf("the request path %q is not canonical", req.URL.Path)),
			Codecs, schema.GroupVersion{}, w, req)
		return
	}

	path := strings.TrimPrefix(req.URL.Path, "/openapi/v3/")
	if !h.owns(path) {
		responsewriters.ErrorNegotiated(
			apierrors.NewNotFound(schema.GroupResource{Resource: "openapi"}, req.URL.Path),
			Codecs, schema.GroupVersion{}, w, req)
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), specFetchTimeout)
	defer cancel()

	fetcher := h.sourceFor(groupFromDocumentPath(req.URL.Path))
	if fetcher == nil {
		responsewriters.ErrorNegotiated(
			apierrors.NewNotFound(schema.GroupResource{Resource: "openapi"}, req.URL.Path),
			Codecs, schema.GroupVersion{}, w, req)
		return
	}

	// The hash query parameter is kcp's cache buster, not a selector, so the
	// document is fetched by path alone.
	raw, err := fetcher.fetchSpec(ctx, req.URL.Path)
	if err != nil {
		h.writeUnavailable(fmt.Errorf("fetching %s from kcp: %w", req.URL.Path, err), w, req)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// sources is every workspace's side channel, or the one a test supplied.
func (h *openAPIHandler) sources() []namedFetcher {
	if h.fetchers == nil {
		return nil
	}
	return h.fetchers()
}

// sourceFor is the side channel for a group, or nothing when no workspace
// serves it.
func (h *openAPIHandler) sourceFor(group string) specFetcher {
	if h.fetcher == nil || group == "" {
		return nil
	}
	return h.fetcher(group)
}

// groupFromDocumentPath picks the group out of /openapi/v3/apis/<group>/<version>.
func groupFromDocumentPath(path string) string {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 4 || segments[0] != "openapi" || segments[2] != "apis" {
		return ""
	}
	return segments[3]
}

// owns reports whether an OpenAPI v3 path, in the form apis/<group>/<version>,
// belongs to a group spillway serves.
func (h *openAPIHandler) owns(path string) bool {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 3 || segments[0] != "apis" {
		return false
	}
	if !h.groups().Has(segments[1]) {
		return false
	}

	// Only advertise a version the workspace actually serves, so the index
	// cannot outlive a version that has been removed.
	_, found := h.cache.Snapshot().Resources[schema.GroupVersion{Group: segments[1], Version: segments[2]}]
	return found
}

func (h *openAPIHandler) writeUnavailable(err error, w http.ResponseWriter, req *http.Request) {
	responsewriters.ErrorNegotiated(
		apierrors.NewServiceUnavailable(err.Error()),
		Codecs, schema.GroupVersion{}, w, req)
}

// serveV2 answers /openapi/v2, built from the workspace's CustomResourceDefinitions.
//
// v2 cannot be proxied the way v3 is. kcp's own v2 document describes only its
// built-in types -- 127 paths, none of them a custom resource -- so there is
// nothing there to forward. The schemas do exist, in the CRDs, which is where
// apiextensions-apiserver builds its own v2 from, so the same builder is used
// here.
//
// Clients new enough to use v3 never come here. The ones that do need this for
// kubectl explain and client-side apply.
func (h *openAPIHandler) serveV2(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), specFetchTimeout)
	defer cancel()

	encoded, err := h.v2.get(ctx, h.currentGeneration())
	if err != nil {
		h.writeUnavailable(err, w, req)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

// buildV2 assembles the v2 document from the CRDs of every workspace.
//
// A workspace that cannot answer is left out rather than taking the document
// with it. There is one v2 document for the whole server, so failing the build
// because one source is unavailable removes kubectl explain and client-side
// apply for every group spillway serves, including the ones that were fine --
// and one source that will never answer is a normal configuration, not an
// outage: an APIExport's virtual endpoint serves the exported group and kcp's
// own, not apiextensions.
//
// Every source failing is different. Then there is nothing to describe, and the
// error is returned so the cache keeps serving the last good document instead
// of publishing one that says these groups have no schemas.
func (h *openAPIHandler) buildV2(ctx context.Context) ([]byte, error) {
	log := klog.FromContext(ctx)

	var definitions []apiextensionsv1.CustomResourceDefinition
	sources := h.sources()
	failed := 0

	for _, source := range sources {
		raw, err := source.fetcher.fetchSpec(ctx, crdPath)
		if err != nil {
			failed++
			// A workspace with no CustomResourceDefinitions API is not broken,
			// it is a different kind of workspace, so it is not reported as a
			// fault every time the aggregation layer polls.
			if notFound(err) {
				log.V(2).Info("Workspace serves no CustomResourceDefinitions; leaving it out of the OpenAPI v2 document",
					"workspace", source.name)
			} else {
				log.Error(err, "Leaving a workspace out of the OpenAPI v2 document", "workspace", source.name)
			}
			continue
		}

		list, err := decodeCRDList(raw)
		if err != nil {
			failed++
			log.Error(err, "Leaving a workspace out of the OpenAPI v2 document", "workspace", source.name)
			continue
		}
		definitions = append(definitions, list...)
	}

	if len(sources) > 0 && failed == len(sources) {
		return nil, fmt.Errorf("no workspace could be asked for its CustomResourceDefinitions")
	}

	document, err := buildV2(definitions, h.groups())
	if err != nil {
		return nil, fmt.Errorf("building the OpenAPI v2 document: %w", err)
	}

	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encoding the OpenAPI v2 document: %w", err)
	}
	return encoded, nil
}

// crdPath is where the workspace lists its CustomResourceDefinitions.
const crdPath = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions"

// notFound reports whether kcp answered that there is nothing there, as opposed
// to failing to answer.
func notFound(err error) bool {
	status := &backendStatusError{}
	if !errors.As(err, &status) {
		return false
	}
	return status.status == http.StatusNotFound
}

// decodeCRDList reads what a workspace answered for its CustomResourceDefinitions.
func decodeCRDList(raw []byte) ([]apiextensionsv1.CustomResourceDefinition, error) {
	list := &apiextensionsv1.CustomResourceDefinitionList{}
	if err := json.Unmarshal(raw, list); err != nil {
		return nil, fmt.Errorf("parsing the CustomResourceDefinition list: %w", err)
	}
	return list.Items, nil
}

// buildV2 turns the workspaces' CRDs into a single v2 document covering the
// owned groups, and nothing else. A definition merged into the cluster's spec
// for a type the cluster does not serve would be worse than no document at all.
func buildV2(definitions []apiextensionsv1.CustomResourceDefinition, served sets.Set[string]) (*spec.Swagger, error) {
	document := &spec.Swagger{SwaggerProps: spec.SwaggerProps{
		Swagger: "2.0",
		Info: &spec.Info{InfoProps: spec.InfoProps{
			Title:   "Kubernetes APIs served from kcp",
			Version: "v0.1.0",
		}},
		Paths:       &spec.Paths{Paths: map[string]spec.PathItem{}},
		Definitions: spec.Definitions{},
	}}

	for i := range definitions {
		crd := &definitions[i]
		if !served.Has(crd.Spec.Group) {
			continue
		}

		for _, version := range crd.Spec.Versions {
			if !version.Served {
				continue
			}
			built, err := builder.BuildOpenAPIV2(crd, version.Name, builder.Options{
				V2: true,
				// The same allowances apiextensions makes for a CRD whose
				// schema is not fully structural; refusing to describe such a
				// resource at all would be worse than describing it loosely.
				AllowNonStructural: true,
			})
			if err != nil {
				return nil, fmt.Errorf("building v2 for %s/%s: %w", crd.Name, version.Name, err)
			}
			if err := aggregator.MergeSpecs(document, built); err != nil {
				return nil, fmt.Errorf("merging v2 for %s/%s: %w", crd.Name, version.Name, err)
			}
		}
	}

	return document, nil
}
