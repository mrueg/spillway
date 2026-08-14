package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilproxy "k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	apiserverproxy "k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

const (
	// dialTimeout bounds establishing a connection to kcp, as opposed to
	// waiting for an answer over one.
	dialTimeout = 10 * time.Second

	// Proxying is the hot path, so connections are worth keeping around.
	maxIdleConnsPerHost = 25
)

// resourceProxy forwards resource requests for the offloaded groups to the kcp
// workspace that stores them.
//
// Authentication and authorization have already happened by the time a request
// reaches here: the generic handler chain authenticates the caller against the
// workload cluster and runs a SubjectAccessReview against its RBAC. What
// remains is the question of who spillway is when it talks to kcp, which
// impersonate decides.
type resourceProxy struct {
	// location is the workspace base URL, including the /clusters/<name> path
	// segment that selects the workspace.
	location *url.URL

	transport http.RoundTripper

	// impersonate forwards the caller's identity to kcp so that the workspace's
	// own RBAC applies as a second gate. When false, requests reach kcp as
	// spillway itself and the workload cluster's RBAC is the only check.
	impersonate bool

	// timeout bounds a single request to kcp. Watches are exempt.
	timeout time.Duration

	// served is the set of API groups spillway offloads, which decides whether
	// an ownerReference could name something the workspace contains.
	served func() sets.Set[string]

	// admission runs the workload cluster's webhooks over writes.
	admission *admissionGate

	// namespaces brings a namespace into the workspace the first time
	// something is written into it. Nil when that is not asked for.
	namespaces *namespaceMirror

	// backend is the transport to kcp, kept as its own type so the circuit
	// breaker's state can be reported.
	backend *backendTransport

	// credentials carry spillway's identity to the workspace and can be
	// replaced without a restart.
	credentials *credentialSource
}

func newResourceProxy(workspace, kubeconfig string, config *rest.Config, impersonate bool, options BackendOptions, served func() sets.Set[string]) (*resourceProxy, error) {
	location, err := url.Parse(config.Host)
	if err != nil {
		return nil, fmt.Errorf("parsing the kcp server URL %q: %w", config.Host, err)
	}
	if location.Scheme == "" || location.Host == "" {
		return nil, fmt.Errorf("the kcp server URL %q must be absolute", config.Host)
	}

	roundTripper, credentials, err := backendRoundTripper(workspace, kubeconfig, config, options)
	if err != nil {
		return nil, err
	}

	return &resourceProxy{
		credentials: credentials,
		location:    location,
		transport:   roundTripper,
		backend:     roundTripper,
		impersonate: impersonate,
		timeout:     options.RequestTimeout,
		served:      served,
	}, nil
}

// backendRoundTripper builds the transport to kcp: TLS and credentials from the
// kubeconfig, then the timeouts, retries, circuit breaker and metrics.
//
// The transport is built here rather than taken from rest.TransportFor so that
// ResponseHeaderTimeout can be set. It is the one timeout that applies to a
// watch as well: a watch may stream for hours, but it still has to start
// answering promptly, and without this a kcp that accepts connections and then
// goes silent would hold every watch open forever.
func backendRoundTripper(workspace, path string, config *rest.Config, options BackendOptions) (*backendTransport, *credentialSource, error) {
	credentials, err := newCredentialSource(path, config, options)
	if err != nil {
		return nil, nil, err
	}
	// The breaker, the retries and the metrics wrap the credentials rather than
	// the other way round, so rotating them does not reset what spillway knows
	// about whether kcp is answering.
	return newBackendTransport(workspace, credentials, options), credentials, nil
}

// credentialedRoundTripper is the part that carries spillway's identity: TLS
// and credentials from the kubeconfig, and the timeouts that bound a request.
func credentialedRoundTripper(config *rest.Config, options BackendOptions) (http.RoundTripper, error) {
	tlsConfig, err := rest.TLSConfigFor(config)
	if err != nil {
		return nil, fmt.Errorf("building the TLS configuration for kcp: %w", err)
	}

	base := utilnet.SetTransportDefaults(&http.Transport{
		TLSClientConfig:       tlsConfig,
		ResponseHeaderTimeout: options.RequestTimeout,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
	})

	credentialed, err := rest.HTTPWrappersForConfig(config, base)
	if err != nil {
		return nil, fmt.Errorf("applying the kcp credentials to the transport: %w", err)
	}
	return credentialed, nil
}

// targetFor maps an incoming request onto the workspace, prepending the
// /clusters/<name> prefix that selects it.
func (p *resourceProxy) targetFor(req *http.Request) *url.URL {
	target := *p.location
	target.Path = path.Join(p.location.Path, req.URL.Path)
	target.RawQuery = req.URL.RawQuery
	return &target
}

// writesObject reports whether the request carries or removes an object, which
// is what admission and the ownerReference check apply to.
func writesObject(req *http.Request) bool {
	switch req.Method {
	case http.MethodPost, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

// canonicalPath reports whether a request path is already in canonical form.
//
// Anything else -- a "." or ".." segment, a doubled slash -- is refused rather
// than normalised. The mux this handler is registered on dispatches by raw
// prefix match, with no cleaning and no redirect, so a path that *resolves*
// somewhere else still arrives here looking like one of ours. Joining it onto
// the workspace prefix would then address a different workspace, or escape the
// prefix altogether, using spillway's credentials -- and the authorization that
// let the request in was decided from the path as written.
func canonicalPath(requestPath string) bool {
	trimmed := strings.TrimSuffix(requestPath, "/")
	if trimmed == "" {
		return requestPath == "/"
	}
	return path.Clean(trimmed) == trimmed
}

func (p *resourceProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !canonicalPath(req.URL.Path) {
		responsewriters.ErrorNegotiated(
			apierrors.NewBadRequest(fmt.Sprintf("the request path %q is not canonical", req.URL.Path)),
			Codecs, schema.GroupVersion{}, w, req)
		return
	}

	// A watch is long lived by design, so a deadline would cut it off mid
	// stream. It is bounded by ResponseHeaderTimeout instead.
	if p.timeout > 0 && !isWatch(req) {
		ctx, cancel := context.WithTimeout(req.Context(), p.timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	// A write is inspected before it reaches kcp: an ownerReference pointing
	// outside the workspace would otherwise be collected by kcp's own garbage
	// collector within seconds.
	if writesObject(req) {
		object, body, err := inspectWrite(req)
		if err != nil {
			responsewriters.ErrorNegotiated(apierrors.NewBadRequest(err.Error()), Codecs, schema.GroupVersion{}, w, req)
			return
		}
		if err := checkOwnerReferences(object, p.served()); err != nil {
			responsewriters.ErrorNegotiated(
				apierrors.NewInvalid(
					schema.GroupKind{Group: object.GroupVersionKind().Group, Kind: object.GetKind()},
					object.GetName(),
					field.ErrorList{field.Invalid(
						field.NewPath("metadata", "ownerReferences"), object.GetOwnerReferences(), err.Error()),
					}),
				Codecs, schema.GroupVersion{}, w, req)
			return
		}

		// The workload cluster's webhooks decide before anything reaches kcp.
		mutated, err := p.admission.run(req, object)
		if err != nil {
			responsewriters.ErrorNegotiated(err, Codecs, schema.GroupVersion{}, w, req)
			return
		}
		if mutated != nil {
			if body, err = json.Marshal(mutated.Object); err != nil {
				responsewriters.ErrorNegotiated(
					apierrors.NewInternalError(fmt.Errorf("re-encoding the admitted object: %w", err)),
					Codecs, schema.GroupVersion{}, w, req)
				return
			}
		}
		if body != nil {
			restoreBody(req, body)
		}
	}

	// A patch is admitted on the object it would produce, which only kcp can
	// work out, so this costs a round trip and is skipped when nothing is
	// listening.
	if req.Method == http.MethodPatch {
		_, patch, err := inspectWrite(req)
		if err != nil {
			responsewriters.ErrorNegotiated(apierrors.NewBadRequest(err.Error()), Codecs, schema.GroupVersion{}, w, req)
			return
		}
		decision, err := p.admission.runPatch(req, patch)
		if err != nil {
			responsewriters.ErrorNegotiated(err, Codecs, schema.GroupVersion{}, w, req)
			return
		}
		if decision != nil && decision.replacement != nil {
			// A mutating webhook changed something the patch cannot express, so
			// the admitted object is written instead.
			req.Method = http.MethodPut
			req.Header.Set("Content-Type", "application/json")
			patch = decision.replacement
		}
		if patch != nil {
			restoreBody(req, patch)
		}
	}

	// Before the write reaches kcp, and after admission, because admission is
	// what has already established that the namespace exists in this cluster.
	if p.namespaces != nil && mirrorsNamespace(req) {
		if info, found := genericapirequest.RequestInfoFrom(req.Context()); found && info.IsResourceRequest {
			p.namespaces.ensure(req.Context(), info.Namespace)
		}
	}

	target := p.targetFor(req)

	// Belt and braces: whatever the path was, the request must still be
	// addressed to the workspace spillway was given.
	if !strings.HasPrefix(target.Path, p.location.Path) {
		responsewriters.ErrorNegotiated(
			apierrors.NewBadRequest(fmt.Sprintf("the request path %q does not address the configured workspace", req.URL.Path)),
			Codecs, schema.GroupVersion{}, w, req)
		return
	}

	roundTripper := p.transport
	if p.impersonate {
		caller, found := genericapirequest.UserFrom(req.Context())
		if !found {
			responsewriters.InternalError(w, req, errors.New("no authenticated user on a request that reached the proxy"))
			return
		}
		roundTripper = transport.NewImpersonatingRoundTripper(transport.ImpersonationConfig{
			UserName: caller.GetName(),
			UID:      caller.GetUID(),
			Groups:   caller.GetGroups(),
			Extra:    caller.GetExtra(),
		}, roundTripper)
	}

	proxyReq, cancel := apiserverproxy.NewRequestForProxy(target, req)
	defer cancel()

	// The caller's credentials are for the workload cluster and mean nothing to
	// kcp. They also have to go because client-go only applies spillway's own
	// bearer token when Authorization is unset, so leaving them would send the
	// request to kcp unauthenticated as far as kcp is concerned.
	proxyReq.Header.Del("Authorization")

	// wrapTransport is off: it rewrites redirects and locations for a proxy
	// whose target has no path prefix, which is not true here -- the workspace
	// path is prepended to every request.
	handler := utilproxy.NewUpgradeAwareHandler(target, roundTripper, false, false, proxyResponder{})
	handler.ServeHTTP(w, proxyReq)
}

// isWatch reports whether the request is a watch, which is the one shape that
// must not be given a deadline.
func isWatch(req *http.Request) bool {
	if info, found := genericapirequest.RequestInfoFrom(req.Context()); found {
		return info.Verb == "watch"
	}
	// Without a RequestInfo, fall back to what the client asked for.
	return req.URL.Query().Get("watch") == "true" || req.URL.Query().Get("watch") == "1"
}

// proxyResponder turns transport level failures into an API error, so clients
// see a Status rather than a bare Go error.
type proxyResponder struct{}

func (proxyResponder) Error(w http.ResponseWriter, req *http.Request, err error) {
	responsewriters.ErrorNegotiated(
		apierrors.NewServiceUnavailable(fmt.Sprintf("kcp is unavailable: %v", err)),
		Codecs, schema.GroupVersion{}, w, req)
}
