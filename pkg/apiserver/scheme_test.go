package apiserver

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The aggregation layer negotiates against the meta types before any offloaded
// group is reachable, so a scheme missing them fails at discovery time rather
// than at compile time.
func TestSchemeServesMetaTypes(t *testing.T) {
	for _, obj := range []struct {
		name string
		kind string
	}{
		{"status", "Status"},
		{"api group list", "APIGroupList"},
		{"api resource list", "APIResourceList"},
	} {
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: obj.kind}
		if _, err := Scheme.New(gvk); err != nil {
			t.Errorf("scheme does not recognize %s (%s): %v", obj.name, gvk, err)
		}
	}
}

func TestCodecsEncodeStatus(t *testing.T) {
	if Codecs.LegacyCodec() == nil {
		t.Fatal("expected a legacy codec for the meta group version")
	}

	if _, _, err := Codecs.UniversalDeserializer().Decode(
		[]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure"}`), nil, &metav1.Status{},
	); err != nil {
		t.Fatalf("decoding a Status failed: %v", err)
	}
}
