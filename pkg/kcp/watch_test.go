package kcp

import (
	"context"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func crd(name, group string) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       apiextensionsv1.CustomResourceDefinitionSpec{Group: group},
	}
}

// signalled reports whether a change arrived within a generous budget. The
// budget is long because it only bounds a failure; a working watch answers in
// milliseconds.
func signalled(changed <-chan struct{}) bool {
	select {
	case <-changed:
		return true
	case <-time.After(15 * time.Second):
		return false
	}
}

func quiet(t *testing.T, changed <-chan struct{}) bool {
	t.Helper()

	select {
	case <-changed:
		return false
	case <-time.After(500 * time.Millisecond):
		return true
	}
}

func TestWatchSignalsOnAnOwnedCRD(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := apiextensionsfake.NewSimpleClientset()
	changed, err := watchCRDs(ctx, client, matcherFor(t, "spillway.example.com"))
	if err != nil {
		t.Fatalf("watchCRDs: %v", err)
	}

	if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx,
		crd("widgets.spillway.example.com", "spillway.example.com"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the CRD: %v", err)
	}

	if !signalled(changed) {
		t.Error("adding a CRD in an owned group produced no change signal")
	}
}

// kcp's own CRDs churn for reasons that have nothing to do with what spillway
// serves; re-reading discovery for each of them would be pure noise.
func TestWatchIgnoresUnownedGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := apiextensionsfake.NewSimpleClientset()
	changed, err := watchCRDs(ctx, client, matcherFor(t, "spillway.example.com"))
	if err != nil {
		t.Fatalf("watchCRDs: %v", err)
	}

	if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx,
		crd("workspaces.tenancy.kcp.io", "tenancy.kcp.io"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the CRD: %v", err)
	}

	if !quiet(t, changed) {
		t.Error("a CRD in an unowned group produced a change signal")
	}
}

func TestWatchSignalsOnDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const name = "widgets.spillway.example.com"
	client := apiextensionsfake.NewSimpleClientset(crd(name, "spillway.example.com"))

	changed, err := watchCRDs(ctx, client, matcherFor(t, "spillway.example.com"))
	if err != nil {
		t.Fatalf("watchCRDs: %v", err)
	}
	// The initial list delivers the existing object; drain it.
	if !signalled(changed) {
		t.Fatal("the initial list produced no signal")
	}

	if err := client.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the CRD: %v", err)
	}

	if !signalled(changed) {
		t.Error("removing a CRD produced no change signal")
	}
}

// The channel holds one pending signal so a burst costs one refresh, not one
// per object.
func TestWatchCoalescesABurst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := apiextensionsfake.NewSimpleClientset()
	changed, err := watchCRDs(ctx, client, matcherFor(t, "spillway.example.com"))
	if err != nil {
		t.Fatalf("watchCRDs: %v", err)
	}

	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx,
			crd(name+".spillway.example.com", "spillway.example.com"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}

	if !signalled(changed) {
		t.Fatal("a burst of CRDs produced no change signal")
	}
	// At most one more signal is pending; five events must not queue five times.
	<-time.After(time.Second)
	depth := len(changed)
	if depth > 1 {
		t.Errorf("%d signals are queued after a burst of five, want at most 1", depth)
	}
}

// A delete the watch missed arrives as a tombstone rather than the object.
func TestCRDFromTombstone(t *testing.T) {
	widget := crd("widgets.spillway.example.com", "spillway.example.com")

	got, ok := crdFrom(cache.DeletedFinalStateUnknown{Key: "widgets", Obj: widget})
	if !ok || got.Spec.Group != "spillway.example.com" {
		t.Errorf("crdFrom(tombstone) = %v, %v; want the wrapped CRD", got, ok)
	}

	if _, ok := crdFrom(cache.DeletedFinalStateUnknown{Key: "widgets", Obj: "not a CRD"}); ok {
		t.Error("crdFrom accepted a tombstone wrapping something that is not a CRD")
	}
	if _, ok := crdFrom("not an object"); ok {
		t.Error("crdFrom accepted something that is not an object at all")
	}
}
