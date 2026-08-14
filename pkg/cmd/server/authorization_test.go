package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// reviewingClient returns a client pointed at a stub apiserver that answers
// every SubjectAccessReview with the given decision, and a count of how many it
// was asked for.
//
// A fake clientset cannot stand in here: the delegated authorizer builds its
// own REST client out of the interface, which the fake does not provide.
func reviewingClient(t *testing.T, allowed bool) (kubernetes.Interface, *atomic.Int64) {
	t.Helper()

	var reviews atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reviews.Add(1)

		review := &authorizationv1.SubjectAccessReview{}
		_ = json.NewDecoder(req.Body).Decode(review)
		review.Status = authorizationv1.SubjectAccessReviewStatus{Allowed: allowed}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(review)
	}))
	t.Cleanup(server.Close)

	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatalf("building the review client: %v", err)
	}
	return client, &reviews
}

func testAuthorizationOptions() *genericoptions.DelegatingAuthorizationOptions {
	options := genericoptions.NewDelegatingAuthorizationOptions()
	options.WithAlwaysAllowPaths("/healthz/groups*")
	return options
}

// Spillway composes the authorizer itself, so the order upstream relies on has
// to be asserted here: an always-allow group is answered before the delegated
// authorizer, not after it.
func TestBuiltAuthorizerShortCircuitsPrivilegedGroups(t *testing.T) {
	client, reviews := reviewingClient(t, false)

	authz, err := buildAuthorizer(testAuthorizationOptions(), client)
	if err != nil {
		t.Fatalf("buildAuthorizer: %v", err)
	}

	decision, _, err := authz.Authorize(context.Background(), authorizer.AttributesRecord{
		User:            &user.DefaultInfo{Name: "admin", Groups: []string{"system:masters"}},
		Verb:            "get",
		Resource:        "widgets",
		APIGroup:        "spillway.example.com",
		Name:            "one",
		ResourceRequest: true,
	})
	if err != nil {
		t.Fatalf("authorizing: %v", err)
	}
	if decision != authorizer.DecisionAllow {
		t.Errorf("decision = %v, want allow for system:masters", decision)
	}
	if reviews.Load() != 0 {
		t.Errorf("%d SubjectAccessReviews sent for system:masters, want 0", reviews.Load())
	}
}

// The health endpoints are reached by probes that hold no credentials, so they
// must stay exempt -- and must not cost a review each.
func TestBuiltAuthorizerAllowsExemptPathsWithoutAReview(t *testing.T) {
	client, reviews := reviewingClient(t, false)

	authz, err := buildAuthorizer(testAuthorizationOptions(), client)
	if err != nil {
		t.Fatalf("buildAuthorizer: %v", err)
	}

	for _, path := range []string{"/healthz", "/readyz", "/livez", "/healthz/groups/spillway.example.com"} {
		decision, _, err := authz.Authorize(context.Background(), authorizer.AttributesRecord{
			User: &user.DefaultInfo{Name: "probe"},
			Path: path,
			Verb: "get",
		})
		if err != nil {
			t.Fatalf("authorizing %s: %v", path, err)
		}
		if decision != authorizer.DecisionAllow {
			t.Errorf("decision for %s = %v, want allow", path, decision)
		}
	}
	if reviews.Load() != 0 {
		t.Errorf("%d SubjectAccessReviews sent for exempt paths, want 0", reviews.Load())
	}
}

// Everything else has to reach the cluster, which is the whole point of a
// delegated authorizer: replacing it must not turn spillway into its own
// authority.
func TestBuiltAuthorizerDelegatesEverythingElse(t *testing.T) {
	for _, allowed := range []bool{true, false} {
		client, reviews := reviewingClient(t, allowed)

		authz, err := buildAuthorizer(testAuthorizationOptions(), client)
		if err != nil {
			t.Fatalf("buildAuthorizer: %v", err)
		}

		decision, _, err := authz.Authorize(context.Background(), authorizer.AttributesRecord{
			User:            &user.DefaultInfo{Name: "someone", Groups: []string{"system:authenticated"}},
			Verb:            "get",
			Resource:        "widgets",
			APIGroup:        "spillway.example.com",
			Name:            "one",
			ResourceRequest: true,
		})
		if err != nil {
			t.Fatalf("authorizing: %v", err)
		}

		want := authorizer.DecisionNoOpinion
		if allowed {
			want = authorizer.DecisionAllow
		}
		if decision != want {
			t.Errorf("cluster said allowed=%v, decision = %v, want %v", allowed, decision, want)
		}
		if reviews.Load() != 1 {
			t.Errorf("%d SubjectAccessReviews sent, want exactly 1", reviews.Load())
		}
	}
}

// The cache the ceiling is about: a repeated request for the same object must
// not send a second review, or raising the rate limit would be treating a
// symptom.
func TestBuiltAuthorizerCachesRepeatedRequests(t *testing.T) {
	client, reviews := reviewingClient(t, true)

	authz, err := buildAuthorizer(testAuthorizationOptions(), client)
	if err != nil {
		t.Fatalf("buildAuthorizer: %v", err)
	}

	attributes := authorizer.AttributesRecord{
		User:            &user.DefaultInfo{Name: "someone"},
		Verb:            "get",
		Resource:        "widgets",
		APIGroup:        "spillway.example.com",
		Name:            "one",
		ResourceRequest: true,
	}
	for range 5 {
		if _, _, err := authz.Authorize(context.Background(), attributes); err != nil {
			t.Fatalf("authorizing: %v", err)
		}
	}
	if reviews.Load() != 1 {
		t.Errorf("%d SubjectAccessReviews for five identical requests, want 1", reviews.Load())
	}

	// A different object is a different cache entry. This is why the rate
	// matters: distinct objects miss, however often they are read.
	attributes.Name = "two"
	if _, _, err := authz.Authorize(context.Background(), attributes); err != nil {
		t.Fatalf("authorizing: %v", err)
	}
	if reviews.Load() != 2 {
		t.Errorf("%d SubjectAccessReviews after naming a second object, want 2", reviews.Load())
	}
}
