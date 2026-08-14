package apiserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
)

func widgetWithOwner(apiVersion, kind, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": testGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name": "owned",
			"ownerReferences": []any{
				map[string]any{
					"apiVersion": apiVersion,
					"kind":       kind,
					"name":       name,
					"uid":        "8b1a9953-0000-0000-0000-000000000000",
				},
			},
		},
	}}
}

// An owner outside the workspace is invisible to kcp's garbage collector, which
// treats it as deleted and collects the object within seconds. Measured against
// a real kcp before this check existed: gone in under five seconds, no event.
func TestRejectsAnOwnerOutsideTheWorkspace(t *testing.T) {
	served := sets.New(testGroup)

	for _, owner := range []struct{ apiVersion, kind string }{
		{"v1", "ConfigMap"},
		{"apps/v1", "Deployment"},
		{"batch/v1", "Job"},
	} {
		err := checkOwnerReferences(widgetWithOwner(owner.apiVersion, owner.kind, "boss"), served)
		if err == nil {
			t.Errorf("an ownerReference to %s %s was accepted; kcp would delete the object",
				owner.apiVersion, owner.kind)
			continue
		}
		if !strings.Contains(err.Error(), "garbage collector") {
			t.Errorf("error for %s does not explain what would happen: %v", owner.apiVersion, err)
		}
	}
}

// An owner in an offloaded group lives in the same workspace, so kcp can see it
// and ordinary ownership works.
func TestAcceptsAnOwnerInTheWorkspace(t *testing.T) {
	served := sets.New(testGroup)

	if err := checkOwnerReferences(widgetWithOwner(testGroup+"/v1alpha1", "Widget", "parent"), served); err != nil {
		t.Errorf("an ownerReference within the offloaded group was rejected: %v", err)
	}
}

func TestAcceptsAnObjectWithoutOwners(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": testGroup + "/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "free"},
	}}

	if err := checkOwnerReferences(object, sets.New(testGroup)); err != nil {
		t.Errorf("an object with no ownerReferences was rejected: %v", err)
	}
}

func TestInspectWriteReturnsTheBodyForForwarding(t *testing.T) {
	const payload = `{"apiVersion":"spillway.example.com/v1alpha1","kind":"Widget"}`

	req := httptest.NewRequest(http.MethodPost, "/apis/g/v1/widgets", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	object, body, err := inspectWrite(req)
	if err != nil {
		t.Fatalf("inspectWrite: %v", err)
	}
	if object == nil || object.GetKind() != "Widget" {
		t.Errorf("decoded %+v, want a Widget", object)
	}
	if string(body) != payload {
		t.Errorf("body = %s, want it returned intact for forwarding", body)
	}

	// The forwarded request must still carry the body.
	restoreBody(req, body)
	forwarded := make([]byte, len(payload))
	if _, err := req.Body.Read(forwarded); err != nil && err.Error() != "EOF" {
		t.Fatalf("reading the restored body: %v", err)
	}
	if string(forwarded) != payload {
		t.Errorf("restored body = %s, want %s", forwarded, payload)
	}
	if req.ContentLength != int64(len(payload)) {
		t.Errorf("ContentLength = %d, want %d", req.ContentLength, len(payload))
	}
}

// A delete or a patch is only decoded to check ownerReferences, so a body that
// cannot carry any is forwarded without being decoded at all.
func TestInspectWriteSkipsTheDecodeWhenOnlyOwnersWouldReadIt(t *testing.T) {
	const options = `{"apiVersion":"meta.k8s.io/v1","kind":"DeleteOptions"}`

	for _, method := range []string{http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/apis/g/v1/widgets/one", strings.NewReader(options))
		req.Header.Set("Content-Type", "application/json")

		object, body, err := inspectWrite(req)
		if err != nil {
			t.Fatalf("%s: inspectWrite: %v", method, err)
		}
		if object != nil {
			t.Errorf("%s: decoded %+v; nothing reads it, so it should have been skipped", method, object)
		}
		if string(body) != options {
			t.Errorf("%s: body = %s, want it returned intact for forwarding", method, body)
		}
	}
}

// Skipping that decode must not open a hole: a body mentioning ownerReferences
// is still decoded and still checked, whatever the method.
func TestInspectWriteStillDecodesBodiesMentioningOwners(t *testing.T) {
	const payload = `{"apiVersion":"spillway.example.com/v1alpha1","kind":"Widget","metadata":{"name":"one",` +
		`"ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"owner","uid":"u"}]}}`

	req := httptest.NewRequest(http.MethodDelete, "/apis/g/v1/widgets/one", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	object, _, err := inspectWrite(req)
	if err != nil {
		t.Fatalf("inspectWrite: %v", err)
	}
	if object == nil {
		t.Fatal("a body carrying ownerReferences was not decoded, so the check could not run")
	}
	if err := checkOwnerReferences(object, sets.New(testGroup)); err == nil {
		t.Error("an owner outside the workspace was accepted; kcp would collect the object")
	}
}

// Anything that is not JSON is not a custom resource, and is passed through
// rather than being decoded.
func TestInspectWriteIgnoresNonJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/apis/g/v1/widgets", strings.NewReader("\x00\x01binary"))
	req.Header.Set("Content-Type", "application/vnd.kubernetes.protobuf")

	object, body, err := inspectWrite(req)
	if err != nil {
		t.Fatalf("inspectWrite: %v", err)
	}
	if object != nil || body != nil {
		t.Errorf("protobuf was decoded (object=%v); it should pass through untouched", object)
	}
}

// A body that is not an object at all is kcp's to reject, not this check's.
func TestInspectWritePassesOnUndecodableJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/apis/g/v1/widgets", strings.NewReader(`["not","an","object"]`))
	req.Header.Set("Content-Type", "application/json")

	object, body, err := inspectWrite(req)
	if err != nil {
		t.Fatalf("inspectWrite: %v", err)
	}
	if object != nil {
		t.Error("a JSON array was treated as an object")
	}
	if body == nil {
		t.Error("the body must still be returned so the request can be forwarded")
	}
}
