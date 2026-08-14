package apiserver

import (
	"context"
	"fmt"
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// conversionPolicy answers whether a resource's versions are interchangeable.
//
// A webhook registered with matchPolicy: Equivalent -- which is the default --
// is shown the object in the version its rule names, whatever version the
// request arrived in. Somebody has to convert. kube-apiserver holds the
// CustomResourceDefinition and does it; spillway holds neither the definition
// nor, for a resource with a conversion webhook, any way to run one, since that
// webhook belongs to the workspace and is dialled by kcp.
//
// What spillway can do is tell the two cases apart. A definition whose
// conversion strategy is None declares its versions structurally identical --
// that is what None means, and it is why kube-apiserver's own converter for
// those does nothing but relabel. Doing the same here is not an approximation
// of the conversion, it is the conversion.
//
// A definition with a webhook converter is refused, as everything was before.
// Handing a policy webhook an object labelled v1 but shaped like v1alpha1 would
// be worse than refusing: it would evaluate the wrong object and say yes.
type conversionPolicy struct {
	// definitions lists the workspace's CustomResourceDefinitions.
	definitions func(ctx context.Context) ([]byte, error)

	// generation reports the API surface, so what is held can be recognised as
	// out of date.
	generation func() uint64

	mu         sync.Mutex
	cached     map[schema.GroupKind]bool
	cachedFrom uint64
	loaded     bool
}

// relabels reports whether an object of this kind may be presented as another
// version of itself by rewriting its apiVersion.
//
// The definitions are fetched on demand rather than kept current, because this
// is asked only when a webhook wants a version other than the request's, which
// is rare. Unknown is false: a kind spillway cannot find a definition for is one
// it cannot make this promise about, which covers a resource bound from an
// APIExport as well as a workspace it cannot reach.
func (p *conversionPolicy) relabels(kind schema.GroupKind) bool {
	if p == nil || p.definitions == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	generation := uint64(0)
	if p.generation != nil {
		generation = p.generation()
	}

	if !p.loaded || p.cachedFrom != generation {
		// A side call to the workspace made in the middle of somebody's
		// request, so it gets its own bounded context rather than theirs: a
		// slow kcp must not hold a webhook open.
		ctx, cancel := context.WithTimeout(context.Background(), specFetchTimeout)
		defer cancel()

		raw, err := p.definitions(ctx)
		if err != nil {
			// Deliberately not cached. A failure now must not become a
			// permanent no for the life of this generation.
			return false
		}
		definitions, err := decodeCRDList(raw)
		if err != nil {
			return false
		}

		p.cached = make(map[schema.GroupKind]bool, len(definitions))
		for i := range definitions {
			crd := &definitions[i]
			p.cached[schema.GroupKind{Group: crd.Spec.Group, Kind: crd.Spec.Names.Kind}] = relabelable(crd)
		}
		p.cachedFrom = generation
		p.loaded = true
	}

	return p.cached[kind]
}

// relabelable reports whether a definition declares its versions
// interchangeable. No conversion stanza means None, which is what the API
// defaults it to.
func relabelable(crd *apiextensionsv1.CustomResourceDefinition) bool {
	return crd.Spec.Conversion == nil || crd.Spec.Conversion.Strategy == apiextensionsv1.NoneConverter
}

// versionConvertor presents an object as another version of itself where the
// workspace says the versions are the same, and refuses otherwise.
//
// Only Convert is on the path that matters: admission builds the target object
// from the creator and asks for the contents to be put into it.
type versionConvertor struct {
	policy *conversionPolicy
}

func (c versionConvertor) Convert(in, out, _ any) error {
	source, ok := in.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("spillway can only convert unstructured objects, not %T", in)
	}
	target, ok := out.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("spillway can only convert into unstructured objects, not %T", out)
	}

	from, to := source.GroupVersionKind(), target.GroupVersionKind()
	if from == to {
		target.Object = source.DeepCopy().Object
		return nil
	}
	if from.Group != to.Group || from.Kind != to.Kind {
		return fmt.Errorf("spillway does not convert %s to %s", from, to)
	}

	if !c.policy.relabels(schema.GroupKind{Group: from.Group, Kind: from.Kind}) {
		return fmt.Errorf("spillway cannot present %s as %s: the workspace's definition of %s does "+
			"not declare its versions interchangeable, so converting between them is its own "+
			"conversion webhook's work, which kcp performs and spillway cannot",
			from, to, from.Kind)
	}

	// None means the versions are the same shape, so the conversion is the
	// label. The copy matters: the caller still holds the object that is going
	// to be stored, and admission may mutate what it is given.
	target.Object = source.DeepCopy().Object
	target.SetGroupVersionKind(to)
	return nil
}

// ConvertToVersion is not used by the admission path, which converts through
// Convert. It answers the identity case and refuses the rest rather than
// guessing at a caller nothing here has.
func (versionConvertor) ConvertToVersion(in runtime.Object, target runtime.GroupVersioner) (runtime.Object, error) {
	current := in.GetObjectKind().GroupVersionKind()
	if _, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{current}); ok {
		return in, nil
	}
	return nil, fmt.Errorf("spillway does not convert %s to %s", current, target)
}

func (versionConvertor) ConvertFieldLabel(_ schema.GroupVersionKind, label, value string) (string, string, error) {
	return label, value, nil
}
