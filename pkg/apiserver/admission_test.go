package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// fakeBackend stands in for kcp: what it currently holds, and what it says a
// patch would produce.
type fakeBackend struct {
	// dryRunQuery is what the resolution was asked with, which has to carry the
	// caller's own parameters.
	dryRunQuery url.Values

	fetched   bool
	current   string
	resolved  string
	patchErr  error
	patchSeen []byte
}

func (b *fakeBackend) fetch(context.Context, string) ([]byte, error) {
	b.fetched = true
	if b.current == "" {
		return nil, errors.New("not found")
	}
	return []byte(b.current), nil
}

func (b *fakeBackend) patchDryRun(_ context.Context, _ string, query url.Values, _ string, patch []byte) ([]byte, error) {
	b.dryRunQuery = query
	b.patchSeen = patch
	if b.patchErr != nil {
		return nil, b.patchErr
	}
	return []byte(b.resolved), nil
}

// recordingAdmission stands in for the chain the workload cluster builds.
type recordingAdmission struct {
	seen     admission.Attributes
	mutate   func(*unstructured.Unstructured)
	rejectOn error
}

func (a *recordingAdmission) Handles(admission.Operation) bool { return true }

func (a *recordingAdmission) Admit(_ context.Context, attrs admission.Attributes, _ admission.ObjectInterfaces) error {
	a.seen = attrs
	if a.mutate != nil {
		if object, ok := attrs.GetObject().(*unstructured.Unstructured); ok {
			a.mutate(object)
		}
	}
	return nil
}

func (a *recordingAdmission) Validate(_ context.Context, attrs admission.Attributes, _ admission.ObjectInterfaces) error {
	a.seen = attrs
	return a.rejectOn
}

func writeRequest(t *testing.T, method, path, verb string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	ctx := genericapirequest.WithRequestInfo(req.Context(), &genericapirequest.RequestInfo{
		IsResourceRequest: true,
		Verb:              verb,
		APIGroup:          testGroup,
		APIVersion:        "v1alpha1",
		Resource:          "widgets",
		Namespace:         "default",
		Name:              "red-widget",
	})
	ctx = genericapirequest.WithUser(ctx, &user.DefaultInfo{Name: "alice", Groups: []string{"devs"}})
	return req.WithContext(ctx)
}

func widgetObject(color string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": testGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "red-widget", "namespace": "default"},
		"spec":       map[string]any{"color": color},
	}}
}

func TestAdmissionRunsOnCreate(t *testing.T) {
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, nil, nil, nil)

	_, err := gate.run(writeRequest(t, http.MethodPost, "/apis/g/v1alpha1/namespaces/default/widgets", "create"),
		widgetObject("red"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if chain.seen == nil {
		t.Fatal("admission was not called for a create")
	}
	if chain.seen.GetOperation() != admission.Create {
		t.Errorf("operation = %s, want CREATE", chain.seen.GetOperation())
	}
	if chain.seen.GetOldObject() != nil {
		t.Error("a create presented an old object")
	}
	if chain.seen.GetUserInfo().GetName() != "alice" {
		t.Errorf("user = %q, want the caller", chain.seen.GetUserInfo().GetName())
	}
	if chain.seen.GetKind().Kind != "Widget" {
		t.Errorf("kind = %s, want Widget", chain.seen.GetKind())
	}
}

// A mutating webhook edits the object in place; whatever it leaves behind is
// what has to reach kcp.
func TestAdmissionForwardsTheMutatedObject(t *testing.T) {
	chain := &recordingAdmission{mutate: func(object *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(object.Object, "green", "spec", "color")
	}}
	gate := newAdmissionGate(chain, nil, nil, nil)

	mutated, err := gate.run(writeRequest(t, http.MethodPost, "/apis/g/v1alpha1/namespaces/default/widgets", "create"),
		widgetObject("red"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if mutated == nil {
		t.Fatal("no object was returned to forward")
	}
	if color, _, _ := unstructured.NestedString(mutated.Object, "spec", "color"); color != "green" {
		t.Errorf("spec.color = %q, want the webhook's value", color)
	}
}

// Update webhooks are written against the old object; presenting nothing would
// report every update as a change from empty.
func TestAdmissionPresentsTheOldObjectOnUpdate(t *testing.T) {
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, &fakeBackend{current: `{"apiVersion":"` + testGroup + `/v1alpha1","kind":"Widget",
			"metadata":{"name":"red-widget","resourceVersion":"7"},"spec":{"color":"blue"}}`}, nil, nil)

	if _, err := gate.run(
		writeRequest(t, http.MethodPut, "/apis/g/v1alpha1/namespaces/default/widgets/red-widget", "update"),
		widgetObject("red")); err != nil {
		t.Fatalf("run: %v", err)
	}

	old, ok := chain.seen.GetOldObject().(*unstructured.Unstructured)
	if !ok || old == nil {
		t.Fatalf("no old object was presented: %#v", chain.seen.GetOldObject())
	}
	if color, _, _ := unstructured.NestedString(old.Object, "spec", "color"); color != "blue" {
		t.Errorf("old spec.color = %q, want what kcp holds", color)
	}
}

// On delete there is no incoming object, and the webhook sees what is going
// away.
func TestAdmissionOnDelete(t *testing.T) {
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, &fakeBackend{
		current: `{"apiVersion":"` + testGroup + `/v1alpha1","kind":"Widget","metadata":{"name":"red-widget"}}`}, nil, nil)

	object, err := gate.run(
		writeRequest(t, http.MethodDelete, "/apis/g/v1alpha1/namespaces/default/widgets/red-widget", "delete"), nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if object != nil {
		t.Error("a delete produced an object to forward")
	}
	if chain.seen.GetOperation() != admission.Delete {
		t.Errorf("operation = %s, want DELETE", chain.seen.GetOperation())
	}
	if chain.seen.GetObject() != nil {
		t.Error("a delete presented an incoming object")
	}
	if chain.seen.GetOldObject() == nil {
		t.Error("a delete did not present the object being removed")
	}
}

// kube-apiserver applies a patch and then admits the result. Spillway forwards
// the patch untouched, so admitting the unpatched object would be worse than
// not admitting at all.
func TestAdmissionSkipsPatch(t *testing.T) {
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, nil, nil, nil)

	if _, err := gate.run(
		writeRequest(t, http.MethodPatch, "/apis/g/v1alpha1/namespaces/default/widgets/red-widget", "patch"),
		widgetObject("red")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if chain.seen != nil {
		t.Error("a patch was put through admission on the unpatched object")
	}
}

func TestAdmissionRejectionSurfaces(t *testing.T) {
	chain := &recordingAdmission{rejectOn: errors.New("denied by policy")}
	gate := newAdmissionGate(chain, nil, nil, nil)

	_, err := gate.run(writeRequest(t, http.MethodPost, "/apis/g/v1alpha1/namespaces/default/widgets", "create"),
		widgetObject("red"))
	if err == nil || err.Error() != "denied by policy" {
		t.Errorf("err = %v, want the webhook's rejection", err)
	}
}

func TestAdmissionWithoutAChainIsANoop(t *testing.T) {
	gate := newAdmissionGate(nil, nil, nil, nil)

	if _, err := gate.run(writeRequest(t, http.MethodPost, "/apis/g/v1alpha1/namespaces/default/widgets", "create"),
		widgetObject("red")); err != nil {
		t.Errorf("run with no admission chain: %v", err)
	}
}

func TestDryRunIsReported(t *testing.T) {
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, nil, nil, nil)

	req := writeRequest(t, http.MethodPost, "/apis/g/v1alpha1/namespaces/default/widgets?dryRun=All", "create")
	if _, err := gate.run(req, widgetObject("red")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !chain.seen.IsDryRun() {
		t.Error("a dry run was presented to the webhook as a real write")
	}
}

func TestOperationMapping(t *testing.T) {
	for verb, want := range map[string]admission.Operation{
		"create": admission.Create,
		"update": admission.Update,
		"delete": admission.Delete,
	} {
		got, handled := operationFor(verb)
		if !handled || got != want {
			t.Errorf("operationFor(%q) = %s/%v, want %s", verb, got, handled, want)
		}
	}
	for _, verb := range []string{"patch", "get", "list", "watch", ""} {
		if _, handled := operationFor(verb); handled {
			t.Errorf("operationFor(%q) is dispatched; only writes with a full object are", verb)
		}
	}
}

const currentWidget = `{"apiVersion":"` + testGroup + `/v1alpha1","kind":"Widget",` +
	`"metadata":{"name":"red-widget","namespace":"default","resourceVersion":"7"},"spec":{"color":"blue","size":3}}`

func patchRequest(t *testing.T) *http.Request {
	t.Helper()

	req := writeRequest(t, http.MethodPatch, "/apis/g/v1alpha1/namespaces/default/widgets/red-widget", "patch")
	req.Header.Set("Content-Type", "application/merge-patch+json")
	return req
}

// A webhook must see the object the patch produces, not the patch. Spillway
// cannot resolve a patch itself, so it asks kcp with a dry run.
func TestPatchIsAdmittedOnTheResolvedObject(t *testing.T) {
	backend := &fakeBackend{
		current: currentWidget,
		resolved: `{"apiVersion":"` + testGroup + `/v1alpha1","kind":"Widget",` +
			`"metadata":{"name":"red-widget","resourceVersion":"7"},"spec":{"color":"green","size":3}}`,
	}
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, backend, nil, nil)

	decision, err := gate.runPatch(patchRequest(t), []byte(`{"spec":{"color":"green"}}`))
	if err != nil {
		t.Fatalf("runPatch: %v", err)
	}
	if decision == nil {
		t.Fatal("the patch was not admitted at all")
	}
	if decision.replacement != nil {
		t.Error("the patch was rewritten even though no webhook changed anything")
	}

	if chain.seen == nil {
		t.Fatal("admission was not called for a patch")
	}
	resolved, ok := chain.seen.GetObject().(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("the webhook saw a %T", chain.seen.GetObject())
	}
	if color, _, _ := unstructured.NestedString(resolved.Object, "spec", "color"); color != "green" {
		t.Errorf("the webhook saw spec.color = %q, want the patched value", color)
	}
	old, ok := chain.seen.GetOldObject().(*unstructured.Unstructured)
	if !ok {
		t.Fatal("no old object was presented for a patch")
	}
	if color, _, _ := unstructured.NestedString(old.Object, "spec", "color"); color != "blue" {
		t.Errorf("the old object had spec.color = %q, want what kcp holds", color)
	}
	if string(backend.patchSeen) != `{"spec":{"color":"green"}}` {
		t.Errorf("the dry run sent %s, want the client's patch verbatim", backend.patchSeen)
	}
}

// When nothing is mutated the original patch is forwarded, so kcp's atomicity
// and conflict behaviour are exactly what they would have been.
func TestUnmutatedPatchIsForwardedUnchanged(t *testing.T) {
	gate := newAdmissionGate(&recordingAdmission{}, &fakeBackend{
		current:  currentWidget,
		resolved: currentWidget,
	}, nil, nil)

	decision, err := gate.runPatch(patchRequest(t), []byte(`{"spec":{"size":3}}`))
	if err != nil {
		t.Fatalf("runPatch: %v", err)
	}
	if decision.replacement != nil {
		t.Error("an unmutated patch was turned into a write of the whole object")
	}
}

// A mutating webhook changes something the patch does not express, so the
// admitted object is written instead -- carrying the resourceVersion it came
// from, or a concurrent write would be silently overwritten.
func TestMutatedPatchBecomesAGuardedWrite(t *testing.T) {
	chain := &recordingAdmission{mutate: func(object *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(object.Object, "audited", "metadata", "labels", "policy")
	}}
	gate := newAdmissionGate(chain, &fakeBackend{current: currentWidget, resolved: currentWidget}, nil, nil)

	decision, err := gate.runPatch(patchRequest(t), []byte(`{"spec":{"size":3}}`))
	if err != nil {
		t.Fatalf("runPatch: %v", err)
	}
	if decision.replacement == nil {
		t.Fatal("a webhook's mutation was dropped: the patch was forwarded as-is")
	}

	written := &unstructured.Unstructured{}
	if err := json.Unmarshal(decision.replacement, &written.Object); err != nil {
		t.Fatalf("decoding the replacement: %v", err)
	}
	if written.GetLabels()["policy"] != "audited" {
		t.Error("the replacement does not carry the webhook's mutation")
	}
	if written.GetResourceVersion() != "7" {
		t.Errorf("resourceVersion = %q, want 7 so a concurrent write conflicts", written.GetResourceVersion())
	}
}

func TestPatchRejectionSurfaces(t *testing.T) {
	gate := newAdmissionGate(&recordingAdmission{rejectOn: errors.New("denied by policy")},
		&fakeBackend{current: currentWidget, resolved: currentWidget}, nil, nil)

	if _, err := gate.runPatch(patchRequest(t), []byte(`{}`)); err == nil {
		t.Error("a webhook rejection of a patch did not surface")
	}
}

// If kcp will not resolve the patch, that is kcp's answer to give, not
// admission's: the request goes on and kcp rejects it in its own words.
func TestPatchFallsThroughWhenKCPRejectsIt(t *testing.T) {
	gate := newAdmissionGate(&recordingAdmission{},
		&fakeBackend{current: currentWidget, patchErr: errors.New("invalid patch")}, nil, nil)

	decision, err := gate.runPatch(patchRequest(t), []byte(`{bad`))
	if err != nil {
		t.Errorf("kcp's rejection was turned into an admission failure: %v", err)
	}
	if decision != nil {
		t.Error("a patch kcp would not resolve was still rewritten")
	}
}

// A patch that creates the object is a create, as it is in kube-apiserver.
func TestPatchThatCreatesIsACreate(t *testing.T) {
	chain := &recordingAdmission{}
	gate := newAdmissionGate(chain, &fakeBackend{resolved: currentWidget}, nil, nil) // nothing there yet

	if _, err := gate.runPatch(patchRequest(t), []byte(`{"spec":{"color":"blue"}}`)); err != nil {
		t.Fatalf("runPatch: %v", err)
	}
	if chain.seen.GetOperation() != admission.Create {
		t.Errorf("operation = %s, want CREATE for a patch that creates", chain.seen.GetOperation())
	}
}

// The dry run costs a round trip, so it is only paid when a webhook could act
// on it.
func TestPatchSkipsTheRoundTripWhenNothingHandlesIt(t *testing.T) {
	backend := &fakeBackend{current: currentWidget, resolved: currentWidget}
	gate := newAdmissionGate(&indifferentAdmission{}, backend, nil, nil)

	if _, err := gate.runPatch(patchRequest(t), []byte(`{}`)); err != nil {
		t.Fatalf("runPatch: %v", err)
	}
	if backend.patchSeen != nil {
		t.Error("kcp was asked to resolve a patch no webhook would have looked at")
	}
}

// indifferentAdmission handles nothing, as an empty chain does.
type indifferentAdmission struct{}

func (indifferentAdmission) Handles(admission.Operation) bool { return false }
func (indifferentAdmission) Admit(context.Context, admission.Attributes, admission.ObjectInterfaces) error {
	return nil
}
func (indifferentAdmission) Validate(context.Context, admission.Attributes, admission.ObjectInterfaces) error {
	return nil
}

// unwatched builds a matcher over a cluster with no webhooks at all, which is
// the case the round trips are being skipped for.
func unwatched(t *testing.T) *admissionMatcher {
	t.Helper()
	return matcherFor(t)
}

// A delete carries no object, so skipping the old-object fetch leaves nothing
// to describe to admission. Getting this wrong made every delete fail with
// "cannot determine the kind" -- caught by the benchmark, 2000 errors in a row.
func TestDeleteWithoutAWatcherIsNotAdmitted(t *testing.T) {
	chain := &recordingAdmission{}
	backend := &fakeBackend{current: currentWidget}
	gate := newAdmissionGate(chain, backend, unwatched(t), nil)

	object, err := gate.run(
		writeRequest(t, http.MethodDelete, "/apis/g/v1alpha1/namespaces/default/widgets/red-widget", "delete"), nil)
	if err != nil {
		t.Fatalf("a delete no webhook watches was rejected: %v", err)
	}
	if object != nil {
		t.Error("a delete produced an object to forward")
	}
	if chain.seen != nil {
		t.Error("admission ran for a delete nothing watches")
	}
	if backend.fetched {
		t.Error("the old object was fetched for a delete nothing watches")
	}
}

// A create carries its object, so admission still runs -- it costs no round
// trip, and the plugins that act on creates should keep acting.
func TestCreateIsStillAdmittedWithoutAWatcher(t *testing.T) {
	chain := &recordingAdmission{}
	backend := &fakeBackend{}
	gate := newAdmissionGate(chain, backend, unwatched(t), nil)

	if _, err := gate.run(
		writeRequest(t, http.MethodPost, "/apis/g/v1alpha1/namespaces/default/widgets", "create"),
		widgetObject("red")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if chain.seen == nil {
		t.Error("admission was skipped for a create; the cheap plugins still have to run")
	}
	if backend.fetched {
		t.Error("a create fetched an old object")
	}
}

// The expensive part of a patch is resolving it against kcp, which is pointless
// when nothing would look at the result.
func TestPatchWithoutAWatcherSkipsTheRoundTrips(t *testing.T) {
	backend := &fakeBackend{current: currentWidget, resolved: currentWidget}
	gate := newAdmissionGate(&recordingAdmission{}, backend, unwatched(t), nil)

	decision, err := gate.runPatch(patchRequest(t), []byte(`{"spec":{"size":3}}`))
	if err != nil {
		t.Fatalf("runPatch: %v", err)
	}
	if decision != nil {
		t.Error("a patch nothing watches was rewritten")
	}
	if backend.patchSeen != nil {
		t.Error("kcp was asked to resolve a patch nothing would look at")
	}
	if backend.fetched {
		t.Error("the old object was fetched for a patch nothing watches")
	}
}

// An apply is refused without a fieldManager, so a dry run that dropped the
// caller's query would fail -- and a failed resolution forwards the write
// unadmitted. That is a server-side apply skipping every webhook, silently.
func TestPatchAdmissionCarriesTheCallersQuery(t *testing.T) {
	backend := &fakeBackend{current: currentWidget, resolved: currentWidget}
	gate := newAdmissionGate(&recordingAdmission{}, backend, nil, nil)

	req := patchRequest(t)
	req.Header.Set("Content-Type", "application/apply-patch+yaml")
	req.URL.RawQuery = "fieldManager=kubectl&fieldValidation=Strict&force=true"

	if _, err := gate.runPatch(req, []byte(`{"spec":{"size":1}}`)); err != nil {
		t.Fatalf("runPatch: %v", err)
	}

	for parameter, want := range map[string]string{
		"fieldManager":    "kubectl",
		"fieldValidation": "Strict",
		"force":           "true",
	} {
		if got := backend.dryRunQuery.Get(parameter); got != want {
			t.Errorf("%s = %q on the dry run, want %q: the resolution has to be asked the same "+
				"question the caller asked", parameter, got, want)
		}
	}
}
