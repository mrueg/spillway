package apiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// maxWriteBytes bounds a request body read for inspection. The API server's own
// limit on an object is well under this.
const maxWriteBytes = 3 << 20

// crossBoundaryOwner explains why an ownerReference cannot point out of the
// workspace.
//
// kcp runs its own garbage collector over the workspace. An ownerReference
// naming something the workspace does not contain looks to it exactly like an
// owner that has been deleted, so it collects the object -- within seconds, with
// no event and no trace. The reference is refused at the door instead, because
// a clear error beats an object that quietly disappears.
//
// Bridging this properly, rather than refusing it, would mean hiding such
// references from kcp and restoring them on the way out, which turns every
// read -- including every watch frame -- into a decode and re-encode. That is a
// different design from the pass-through proxy this is, so it is a decision
// rather than a patch.
func crossBoundaryOwner(owner metav1.OwnerReference, group string) error {
	return fmt.Errorf(
		"ownerReference to %s %q is in group %q, which is not served from the kcp workspace: "+
			"kcp's garbage collector cannot see that owner and would delete this object almost immediately",
		owner.Kind, owner.Name, group)
}

// ownerReferencesField is the only field checkOwnerReferences reads. A body that
// does not contain the string cannot contain the field, so it cannot fail the
// check -- and a body that does contain it as ordinary data only costs the
// decode that would have happened anyway.
var ownerReferencesField = []byte("ownerReferences")

// inspectWrite reads the body of a write so it can be checked before it reaches
// kcp, and returns it so the request can still be forwarded.
//
// Only JSON is inspected. Custom resources are JSON on the wire, so anything
// else is not an object spillway serves and is passed through untouched.
//
// The decode is skipped where nothing but the ownerReference check would read
// the result and the body does not mention ownerReferences. That is a delete,
// whose body is DeleteOptions rather than an object, and a patch, whose caller
// forwards the raw patch. A create or an update is always decoded: admission
// builds its attributes from the object, so the work is not wasted.
func inspectWrite(req *http.Request) (*unstructured.Unstructured, []byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil, nil
	}
	// Custom resources are JSON on the wire. A patch carries its own JSON based
	// content types, which are read too; anything else is not something
	// spillway inspects and is passed through untouched.
	contentType := req.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "application/json") &&
		!strings.HasSuffix(contentType, "+json") && !strings.HasSuffix(contentType, "+yaml") {
		return nil, nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, maxWriteBytes+1))
	_ = req.Body.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the request body: %w", err)
	}
	if len(body) > maxWriteBytes {
		return nil, body, fmt.Errorf("the request body exceeds %d bytes", maxWriteBytes)
	}

	if !decodesObject(req.Method) && !bytes.Contains(body, ownerReferencesField) {
		return nil, body, nil
	}

	object := &unstructured.Unstructured{}
	if err := json.Unmarshal(body, &object.Object); err != nil {
		// Not an object spillway understands. Deciding it is malformed is kcp's
		// job, and its error message is the one the client should get, so the
		// body is passed on rather than rejected here.
		return nil, body, nil //nolint:nilerr // kcp owns this rejection
	}
	return object, body, nil
}

// decodesObject reports whether anything beyond the ownerReference check reads
// the decoded object. Admission needs it to describe a create or an update to a
// webhook; a delete is admitted on the object kcp holds, not on the body, and a
// patch is forwarded as it arrived.
func decodesObject(method string) bool {
	return method == http.MethodPost || method == http.MethodPut
}

// restoreBody puts an already-read body back so the request can be forwarded.
func restoreBody(req *http.Request, body []byte) {
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
}

// checkOwnerReferences refuses an object whose owner lives outside the
// workspace. served is the set of API groups spillway offloads, which is what
// decides whether an owner can exist in the workspace at all.
func checkOwnerReferences(object *unstructured.Unstructured, served sets.Set[string]) error {
	if object == nil {
		return nil
	}

	for _, owner := range object.GetOwnerReferences() {
		gv, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil {
			return fmt.Errorf("ownerReference to %s %q has an unparseable apiVersion %q: %w",
				owner.Kind, owner.Name, owner.APIVersion, err)
		}
		if !served.Has(gv.Group) {
			return crossBoundaryOwner(owner, gv.Group)
		}
	}
	return nil
}
