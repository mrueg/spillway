package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

// admissionGate runs the workload cluster's admission chain over objects whose
// storage is kcp.
//
// A ValidatingWebhookConfiguration in the workload cluster is consumed by that
// cluster's own admission chain, which an aggregated API never passes through:
// kube-apiserver proxies the request rather than admitting it. Spillway is
// itself built on k8s.io/apiserver, and its admission chain is constructed
// against the workload cluster's client and informers, so it reads the same
// webhook configurations and dials the same webhook services. All that was
// missing was the call.
//
// Nothing is copied into the cluster to make this work. A webhook is invoked
// with an AdmissionReview describing the object; the object itself never has to
// exist there.
type admissionGate struct {
	admit      admission.Interface
	interfaces admission.ObjectInterfaces

	// backend reads the current object from kcp, which update and delete need
	// in order to present the old object to a webhook, and resolves a patch
	// into the object it would produce.
	backend patchBackend

	// matcher decides whether any of that work is worth doing.
	matcher *admissionMatcher
}

// patchBackend is the part of the kcp client admission needs.
type patchBackend interface {
	fetch(ctx context.Context, path string) ([]byte, error)
	patchDryRun(ctx context.Context, path string, query url.Values, contentType string, patch []byte) ([]byte, error)
}

func newAdmissionGate(admit admission.Interface, backend patchBackend, matcher *admissionMatcher,
	conversion *conversionPolicy) *admissionGate {
	return &admissionGate{
		admit: admit,
		interfaces: unstructuredInterfaces{
			equivalents: runtime.NewEquivalentResourceRegistry(),
			conversion:  conversion,
		},
		backend: backend,
		matcher: matcher,
	}
}

// resourceFor names the resource under admission, for the matcher.
func resourceFor(info *genericapirequest.RequestInfo) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: info.APIGroup, Version: info.APIVersion, Resource: info.Resource}
}

// operationFor maps a request verb to an admission operation. A patch is not
// here: it is admitted, but by a different route, because what a webhook must
// see is the object the patch produces rather than the patch itself.
func operationFor(verb string) (admission.Operation, bool) {
	switch verb {
	case "create":
		return admission.Create, true
	case "update":
		return admission.Update, true
	case "delete":
		return admission.Delete, true
	default:
		return "", false
	}
}

// patchDecision says how to forward a patch once it has been admitted.
type patchDecision struct {
	// replacement, when set, is an object to PUT instead of forwarding the
	// patch, because a mutating webhook changed something the patch does not
	// express.
	replacement []byte
}

// runPatch admits a patch by asking kcp what the patch would produce.
//
// kube-apiserver applies a patch and then admits the result, so a webhook sees
// the object as it will be stored. Spillway cannot resolve a patch itself: a
// JSON patch, a merge patch and a server-side apply each resolve differently,
// and apply depends on field ownership that only kcp holds. So kcp is asked,
// with a dry run, and the answer is what goes to the webhooks.
//
// If admission leaves that answer alone the original patch is forwarded
// untouched, which keeps kcp's atomicity and conflict behaviour exactly as it
// would have been. Only when a mutating webhook actually changes something does
// the request become a PUT of the admitted object, guarded by the
// resourceVersion it was derived from so a concurrent write still conflicts
// rather than being lost.
func (g *admissionGate) runPatch(req *http.Request, patch []byte) (*patchDecision, error) {
	if g == nil || g.admit == nil || g.backend == nil {
		return nil, nil
	}

	log := klog.FromContext(req.Context())

	info, found := genericapirequest.RequestInfoFrom(req.Context())
	if !found || !info.IsResourceRequest || info.Verb != "patch" {
		log.V(4).Info("patch admission: skipped, not a resource patch",
			"found", found, "verb", verbOf(info), "resourceRequest", isResourceRequest(info))
		return nil, nil
	}
	// Resolving a patch costs two round trips to kcp, so it is only done when a
	// webhook or policy would actually see the result.
	if !g.matcher.matches(resourceFor(info), admission.Update) {
		log.V(4).Info("patch admission: skipped, no webhook matches this resource")
		return nil, nil
	}
	if !g.handlesAnything() {
		log.V(4).Info("patch admission: skipped, nothing handles writes")
		return nil, nil
	}

	caller, found := genericapirequest.UserFrom(req.Context())
	if !found {
		return nil, apierrors.NewInternalError(fmt.Errorf("no authenticated user on a request that reached admission"))
	}

	// What the object is now, and what the patch would make of it.
	old, _ := g.oldObject(req, admission.Update)

	resolvedBody, err := g.backend.patchDryRun(req.Context(), req.URL.Path, req.URL.Query(), req.Header.Get("Content-Type"), patch)
	if err != nil {
		// kcp rejected the patch, or is unreachable. Either way this is not
		// admission's verdict to give: the request is forwarded and kcp gets to
		// answer for itself. It is logged because a patch that quietly skips
		// admission is a policy hole, and silence makes it invisible.
		klog.FromContext(req.Context()).V(2).Info("Could not resolve a patch for admission; forwarding it unadmitted",
			"path", req.URL.Path, "err", err)
		return nil, nil //nolint:nilerr // kcp owns this rejection
	}

	resolved := &unstructured.Unstructured{}
	if err := json.Unmarshal(resolvedBody, &resolved.Object); err != nil {
		return nil, nil //nolint:nilerr // as above
	}

	// A patch that creates the object is a create, as it is in kube-apiserver.
	operation := admission.Update
	if old == nil {
		operation = admission.Create
	}

	before := resolved.DeepCopy()
	attributes := admission.NewAttributesRecord(
		resolved,
		objectOrNil(old),
		resolved.GroupVersionKind(),
		info.Namespace,
		info.Name,
		schema.GroupVersionResource{Group: info.APIGroup, Version: info.APIVersion, Resource: info.Resource},
		info.Subresource,
		operation,
		nil,
		isDryRun(req),
		caller,
	)

	log.V(4).Info("patch admission: admitting", "operation", operation, "kind", resolved.GetKind())

	if mutator, ok := g.admit.(admission.MutationInterface); ok && mutator.Handles(operation) {
		if err := mutator.Admit(req.Context(), attributes, g.interfaces); err != nil {
			return nil, err
		}
	}
	if validator, ok := g.admit.(admission.ValidationInterface); ok && validator.Handles(operation) {
		if err := validator.Validate(req.Context(), attributes, g.interfaces); err != nil {
			return nil, err
		}
	}

	if equality.Semantic.DeepEqual(before.Object, resolved.Object) {
		// Nothing was changed, so the patch itself can go to kcp and behave
		// exactly as it would have.
		return &patchDecision{}, nil
	}

	// A webhook changed something. The patch cannot carry that, so the admitted
	// object is written instead -- with the resourceVersion it came from, so a
	// concurrent write conflicts rather than being overwritten.
	if old != nil {
		resolved.SetResourceVersion(old.GetResourceVersion())
	}
	replacement, err := json.Marshal(resolved.Object)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("re-encoding the admitted object: %w", err))
	}
	return &patchDecision{replacement: replacement}, nil
}

// verbOf and isResourceRequest keep the diagnostic above readable when there is
// no request info at all.
func verbOf(info *genericapirequest.RequestInfo) string {
	if info == nil {
		return ""
	}
	return info.Verb
}

func isResourceRequest(info *genericapirequest.RequestInfo) bool {
	return info != nil && info.IsResourceRequest
}

// handlesAnything reports whether the chain would do anything for a write, so
// the extra round trip to resolve a patch is only paid when it can matter.
func (g *admissionGate) handlesAnything() bool {
	if mutator, ok := g.admit.(admission.MutationInterface); ok && mutator.Handles(admission.Update) {
		return true
	}
	if validator, ok := g.admit.(admission.ValidationInterface); ok && validator.Handles(admission.Update) {
		return true
	}
	return false
}

// run puts the request through admission and reports the object to forward,
// which a mutating webhook may have changed. A nil object means the request
// body should be left as it is.
func (g *admissionGate) run(req *http.Request, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if g == nil || g.admit == nil {
		return nil, nil
	}

	info, found := genericapirequest.RequestInfoFrom(req.Context())
	if !found || !info.IsResourceRequest {
		return nil, nil
	}
	operation, handled := operationFor(info.Verb)
	if !handled {
		return nil, nil
	}

	caller, found := genericapirequest.UserFrom(req.Context())
	if !found {
		return nil, apierrors.NewInternalError(fmt.Errorf("no authenticated user on a request that reached admission"))
	}
	watched := g.matcher.matches(resourceFor(info), operation)

	// A delete carries no object at all, so without the old one there is
	// nothing to describe to admission -- not even a kind. When no webhook or
	// policy would see it, the fetch is skipped and so is admission. The
	// plugins that would still run do not act on deletes: namespace lifecycle
	// refuses creates in a terminating namespace, not removals from one.
	if operation == admission.Delete && !watched {
		return nil, nil
	}

	// The old object exists to be shown to a webhook. Fetching it costs a round
	// trip to kcp, so it is only worth it when one would see it.
	var old *unstructured.Unstructured
	if watched {
		fetched, err := g.oldObject(req, operation)
		if err != nil {
			return nil, err
		}
		old = fetched
	}

	// On delete there is no incoming object; the webhook sees what is about to
	// go away.
	subject := object
	if operation == admission.Delete {
		subject = nil
	}

	kind, err := kindFor(subject, old, info)
	if err != nil {
		return nil, err
	}

	name := info.Name
	if name == "" && subject != nil {
		name = subject.GetName()
	}

	attributes := admission.NewAttributesRecord(
		objectOrNil(subject),
		objectOrNil(old),
		kind,
		info.Namespace,
		name,
		schema.GroupVersionResource{Group: info.APIGroup, Version: info.APIVersion, Resource: info.Resource},
		info.Subresource,
		operation,
		nil,
		isDryRun(req),
		caller,
	)

	if mutator, ok := g.admit.(admission.MutationInterface); ok && mutator.Handles(operation) {
		if err := mutator.Admit(req.Context(), attributes, g.interfaces); err != nil {
			return nil, err
		}
	}
	if validator, ok := g.admit.(admission.ValidationInterface); ok && validator.Handles(operation) {
		if err := validator.Validate(req.Context(), attributes, g.interfaces); err != nil {
			return nil, err
		}
	}

	// A mutating webhook edits the object in place, so what to forward is
	// whatever the attributes hold now.
	if subject == nil {
		return nil, nil
	}
	mutated, ok := attributes.GetObject().(*unstructured.Unstructured)
	if !ok {
		return nil, apierrors.NewInternalError(fmt.Errorf("admission replaced the object with a %T", attributes.GetObject()))
	}
	return mutated, nil
}

// oldObject reads the object as it currently stands in kcp. Update and delete
// webhooks are written against it, and presenting nothing would misreport every
// update as a change from empty.
func (g *admissionGate) oldObject(req *http.Request, operation admission.Operation) (*unstructured.Unstructured, error) {
	if operation == admission.Create || g.backend == nil {
		return nil, nil
	}

	body, err := g.backend.fetch(req.Context(), req.URL.Path)
	if err != nil {
		// The object may simply not be there, which kcp will say better than
		// this can. Admission runs without an old object rather than turning a
		// missing object into an admission failure.
		return nil, nil //nolint:nilerr // kcp owns this rejection
	}

	old := &unstructured.Unstructured{}
	if err := json.Unmarshal(body, &old.Object); err != nil {
		return nil, nil //nolint:nilerr // as above
	}
	return old, nil
}

// kindFor works out the kind under admission. It comes from the object when
// there is one, and from what kcp holds when there is not.
func kindFor(subject, old *unstructured.Unstructured, info *genericapirequest.RequestInfo) (schema.GroupVersionKind, error) {
	for _, candidate := range []*unstructured.Unstructured{subject, old} {
		if candidate != nil {
			if gvk := candidate.GroupVersionKind(); gvk.Kind != "" {
				return gvk, nil
			}
		}
	}
	return schema.GroupVersionKind{}, apierrors.NewBadRequest(
		fmt.Sprintf("cannot determine the kind for %s/%s %s", info.APIGroup, info.APIVersion, info.Resource))
}

// objectOrNil keeps a nil *Unstructured from becoming a non-nil runtime.Object,
// which admission would then try to read.
func objectOrNil(object *unstructured.Unstructured) runtime.Object {
	if object == nil {
		return nil
	}
	return object
}

func isDryRun(req *http.Request) bool {
	for _, value := range req.URL.Query()["dryRun"] {
		if value == "All" {
			return true
		}
	}
	return false
}

// unstructuredInterfaces gives admission the object machinery for resources
// spillway has no Go types for, in the same shape apiextensions-apiserver uses
// for custom resources.
type unstructuredInterfaces struct {
	equivalents runtime.EquivalentResourceMapper

	// conversion decides whether an object may be presented to a webhook as
	// another version of itself.
	conversion *conversionPolicy
}

func (unstructuredInterfaces) GetObjectCreater() runtime.ObjectCreater { return unstructuredCreator{} }

func (unstructuredInterfaces) GetObjectTyper() runtime.ObjectTyper {
	return unstructuredTyper{}
}

func (unstructuredInterfaces) GetObjectDefaulter() runtime.ObjectDefaulter {
	// kcp applies the CRD's defaults; spillway must not apply a second set.
	return noopDefaulter{}
}

func (i unstructuredInterfaces) GetObjectConvertor() runtime.ObjectConvertor {
	return versionConvertor{policy: i.conversion}
}

func (i unstructuredInterfaces) GetEquivalentResourceMapper() runtime.EquivalentResourceMapper {
	return i.equivalents
}

type unstructuredCreator struct{}

func (unstructuredCreator) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(kind)
	return object, nil
}

type unstructuredTyper struct{}

func (unstructuredTyper) ObjectKinds(object runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	gvk := object.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "" {
		return nil, false, runtime.NewMissingKindErr("the object has no kind")
	}
	if gvk.Version == "" {
		return nil, false, runtime.NewMissingVersionErr("the object has no apiVersion")
	}
	return []schema.GroupVersionKind{gvk}, false, nil
}

func (unstructuredTyper) Recognizes(schema.GroupVersionKind) bool { return true }

type noopDefaulter struct{}

func (noopDefaulter) Default(runtime.Object) {}

var _ admission.ObjectInterfaces = unstructuredInterfaces{}
