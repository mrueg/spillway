package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// backendClient makes the small number of side requests spillway needs to make
// on its own behalf, over the same transport as the proxy so that they are
// bounded, retried, counted and cut off by the circuit breaker alongside
// everything else.
type backendClient struct {
	// location is the workspace base URL, including its /clusters/<name> path.
	location *url.URL

	transport http.RoundTripper
}

// fetch reads a document from the workspace on spillway's own behalf.
func (c *backendClient) fetch(ctx context.Context, requestPath string) ([]byte, error) {
	return c.do(ctx, http.MethodGet, requestPath, nil, "", nil, "spillway", "read")
}

// fetchSpec reads an OpenAPI document, counted separately so the aggregator's
// polling is distinguishable from the data path.
func (c *backendClient) fetchSpec(ctx context.Context, requestPath string) ([]byte, error) {
	return c.do(ctx, http.MethodGet, requestPath, nil, "", nil, "openapi", "get")
}

// patchDryRun asks kcp what the object would become, without writing it.
//
// This is how a patch is admitted without reimplementing patch semantics.
// A JSON patch, a merge patch and a server-side apply all resolve differently,
// and apply cannot be resolved outside the server at all -- it depends on field
// ownership kcp holds. Asking the server that owns the object is both simpler
// and the only answer that is right by construction.
// The caller's own query is carried through. It is not decoration: an apply is
// refused outright without a fieldManager, so a dry run that dropped it would
// fail, and a failed resolution means the write is forwarded unadmitted -- a
// server-side apply silently skipping every webhook. The rest matters too, for
// a weaker reason: fieldValidation and force change what the patch produces, and
// the point of the dry run is to show admission the object that will be stored.
func (c *backendClient) patchDryRun(ctx context.Context, requestPath string, caller url.Values, contentType string, patch []byte) ([]byte, error) {
	query := url.Values{}
	for key, values := range caller {
		query[key] = values
	}
	query.Set("dryRun", "All")
	// Labelled apart from the plain read: this is the round trip admission adds
	// to every patch, and telling them apart is the difference between knowing
	// patches are slow and knowing why.
	return c.do(ctx, http.MethodPatch, requestPath, query, contentType, patch, "spillway", "dryrun")
}

// create writes an object the workspace needs but does not have, which today is
// a namespace and nothing else.
func (c *backendClient) create(ctx context.Context, requestPath string, body []byte) ([]byte, error) {
	return c.do(ctx, http.MethodPost, requestPath, nil, "application/json", body, "spillway", "create")
}

func (c *backendClient) do(ctx context.Context, method, requestPath string, query url.Values, contentType string, body []byte, resource, verb string) ([]byte, error) {
	target := *c.location
	target.Path = path.Join(c.location.Path, requestPath)
	if query != nil {
		target.RawQuery = query.Encode()
	}

	// The transport labels its metrics from the request info; these requests
	// are spillway's own, so they are named as such rather than inheriting the
	// caller's.
	ctx = genericapirequest.WithRequestInfo(ctx, &genericapirequest.RequestInfo{
		Verb:     verb,
		Resource: resource,
	})

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("building a %s for %s: %w", method, requestPath, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxSpecBytes))
	if err != nil {
		return nil, fmt.Errorf("reading %s from kcp: %w", requestPath, err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, &backendStatusError{status: resp.StatusCode, body: payload, path: requestPath}
	}
	return payload, nil
}

// backendStatusError carries kcp's own answer, so a rejection can be handed
// back to the client as kcp wrote it rather than reworded.
type backendStatusError struct {
	status int
	body   []byte
	path   string
}

func (e *backendStatusError) Error() string {
	return fmt.Sprintf("kcp answered %d for %s: %s", e.status, e.path, e.body)
}
