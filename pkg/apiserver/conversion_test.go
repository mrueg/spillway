package apiserver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const interchangeableCRDs = `{
  "apiVersion": "apiextensions.k8s.io/v1",
  "kind": "CustomResourceDefinitionList",
  "items": [
    {"spec": {"group": "spillway.example.com", "names": {"kind": "Widget"},
              "conversion": {"strategy": "None"}}},
    {"spec": {"group": "spillway.example.com", "names": {"kind": "Lever"}}},
    {"spec": {"group": "spillway.example.com", "names": {"kind": "Gauge"},
              "conversion": {"strategy": "Webhook"}}}
  ]
}`

func testConversion(list string, err error) *conversionPolicy {
	fetches := 0
	return &conversionPolicy{
		definitions: func(context.Context) ([]byte, error) {
			fetches++
			if err != nil {
				return nil, err
			}
			return []byte(list), nil
		},
		generation: func() uint64 { return 1 },
	}
}

func convert(t *testing.T, policy *conversionPolicy, kind, from, to string) error {
	t.Helper()

	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "spillway.example.com/" + from,
		"kind":       kind,
		"metadata":   map[string]any{"name": "one"},
		"spec":       map[string]any{"colour": "red"},
	}}
	target := &unstructured.Unstructured{}
	target.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "spillway.example.com", Version: to, Kind: kind})

	if err := (versionConvertor{policy: policy}).Convert(source, target, nil); err != nil {
		return err
	}

	if target.GetAPIVersion() != "spillway.example.com/"+to {
		t.Errorf("apiVersion = %q, want the version asked for", target.GetAPIVersion())
	}
	if colour, _, _ := unstructured.NestedString(target.Object, "spec", "colour"); colour != "red" {
		t.Errorf("spec.colour = %q, want the contents carried over", colour)
	}
	// The caller still holds the object that is going to be stored.
	if source.GetAPIVersion() != "spillway.example.com/"+from {
		t.Errorf("the source was relabelled to %q", source.GetAPIVersion())
	}
	return nil
}

// A definition whose strategy is None declares its versions structurally
// identical, which is why relabelling is the conversion rather than an
// approximation of it.
func TestConvertRelabelsWhenTheVersionsAreInterchangeable(t *testing.T) {
	policy := testConversion(interchangeableCRDs, nil)

	if err := convert(t, policy, "Widget", "v1alpha1", "v1beta1"); err != nil {
		t.Errorf("converting a Widget: %v", err)
	}
	// No conversion stanza at all means None, which is the API's default.
	if err := convert(t, policy, "Lever", "v1alpha1", "v1beta1"); err != nil {
		t.Errorf("converting a Lever: %v", err)
	}
}

// Handing a policy webhook an object labelled v1beta1 but shaped like v1alpha1
// would have it evaluate the wrong object and say yes.
func TestConvertRefusesWhenTheWorkspaceConverts(t *testing.T) {
	err := convert(t, testConversion(interchangeableCRDs, nil), "Gauge", "v1alpha1", "v1beta1")
	if err == nil {
		t.Fatal("an object was relabelled for a resource with a conversion webhook")
	}
	if !strings.Contains(err.Error(), "conversion webhook") {
		t.Errorf("error = %v, want it to say whose job the conversion is", err)
	}
}

// A kind with no definition to consult -- one bound from an APIExport, or a
// workspace that cannot be reached -- is one spillway cannot make the promise
// about.
func TestConvertRefusesWhatItCannotCheck(t *testing.T) {
	if err := convert(t, testConversion(interchangeableCRDs, nil), "Unknown", "v1alpha1", "v1beta1"); err == nil {
		t.Error("an unknown kind was relabelled")
	}
	if err := convert(t, testConversion("", errors.New("kcp unreachable")), "Widget", "v1alpha1", "v1beta1"); err == nil {
		t.Error("a kind was relabelled while the workspace could not be read")
	}
	if err := convert(t, nil, "Widget", "v1alpha1", "v1beta1"); err == nil {
		t.Error("a kind was relabelled with no policy at all")
	}
}

// A failure must not become a permanent no for the life of the generation.
func TestConversionDoesNotCacheAFailure(t *testing.T) {
	attempts := 0
	policy := &conversionPolicy{
		definitions: func(context.Context) ([]byte, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("kcp unreachable")
			}
			return []byte(interchangeableCRDs), nil
		},
		generation: func() uint64 { return 1 },
	}

	if err := convert(t, policy, "Widget", "v1alpha1", "v1beta1"); err == nil {
		t.Fatal("the first attempt succeeded despite the workspace being unreachable")
	}
	if err := convert(t, policy, "Widget", "v1alpha1", "v1beta1"); err != nil {
		t.Errorf("the second attempt was refused from a cached failure: %v", err)
	}
}

// Converting between kinds is not a version question and is not something a
// declared strategy says anything about.
func TestConvertRefusesAnotherKind(t *testing.T) {
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "spillway.example.com/v1alpha1", "kind": "Widget",
	}}
	target := &unstructured.Unstructured{}
	target.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "spillway.example.com", Version: "v1alpha1", Kind: "Gauge"})

	if err := (versionConvertor{policy: testConversion(interchangeableCRDs, nil)}).
		Convert(source, target, nil); err == nil {
		t.Error("a Widget was converted into a Gauge")
	}
}

// The identity case has to keep working, since it is most of what admission
// asks for.
func TestConvertCopiesForTheSameVersion(t *testing.T) {
	if err := convert(t, testConversion(interchangeableCRDs, nil), "Widget", "v1alpha1", "v1alpha1"); err != nil {
		t.Errorf("converting to the same version: %v", err)
	}
}
