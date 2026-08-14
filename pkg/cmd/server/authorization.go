package server

import (
	"fmt"

	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	"k8s.io/apiserver/pkg/authorization/path"
	"k8s.io/apiserver/pkg/authorization/union"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Every aggregated API server delegates authorization: it POSTs a
// SubjectAccessReview back to the cluster for each request it cannot answer
// from cache. The client that sends them is built inside the options package
// with a rate limit that no flag reaches:
//
//	// set high qps/burst limits since this will effectively limit API server responsiveness
//	clientConfig.QPS = 200
//	clientConfig.Burst = 400
//
// For most aggregated servers that is generous. For spillway it is a ceiling on
// the whole server, because the authorizer's cache keys on the object name, so
// every request naming a distinct object misses it. Measured on a single node,
// with 8000 objects read once each: get, patch and delete all stopped at 190 to
// 210 operations per second, the SubjectAccessReview rate held at 200.4 per
// second over any 20 second window, and create -- the one verb whose attributes
// carry no name, so all of them share one cache entry -- ran faster at 245.9,
// while the same reads against a native CRD reached 1489.8.
//
// Throttling happens above the transport, in the REST client, so the
// CustomRoundTripperFn hook the options do expose cannot reach it. The only way
// past it is to build the authorizer here, over a client of spillway's own.
const (
	// defaultAuthorizationQPS is four times the upstream default: enough that the
	// limiter sits above what spillway can serve rather than under it.
	//
	// It is not unlimited, and the flag exists rather than the number being
	// raised further, because every review is a request to the very apiserver
	// spillway is meant to be unloading. Raising this trades load there for
	// throughput here.
	defaultAuthorizationQPS   = 800
	defaultAuthorizationBurst = 1600
)

// authorizationClient builds the client that sends SubjectAccessReviews,
// resolved the same way the upstream options resolve theirs.
func (o *SpillwayServerOptions) authorizationClient() (kubernetes.Interface, error) {
	options := o.RecommendedOptions.Authorization

	var config *rest.Config
	var err error

	switch {
	case len(options.RemoteKubeConfigFile) > 0:
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: options.RemoteKubeConfigFile},
			&clientcmd.ConfigOverrides{})
		config, err = loader.ClientConfig()
	default:
		config, err = rest.InClusterConfig()
		if err != nil && options.RemoteKubeConfigFileOptional {
			// Without a way to ask the cluster, there is no delegated authorizer
			// to build and the caller falls back to the upstream wiring, which
			// makes the same allowance.
			return nil, nil //nolint:nilerr // matches upstream's optional handling
		}
	}
	if err != nil {
		return nil, fmt.Errorf("building the client for delegated authorization: %w", err)
	}

	config.QPS = float32(o.AuthorizationQPS)
	config.Burst = o.AuthorizationBurst
	config.Timeout = options.ClientTimeout
	if options.CustomRoundTripperFn != nil {
		config.Wrap(options.CustomRoundTripperFn)
	}

	return kubernetes.NewForConfig(config)
}

// applyAuthorization replaces the authorizer the recommended options installed
// with an identically composed one whose review client is not capped at 200
// requests per second.
//
// The composition is upstream's, in upstream's order, and deliberately so: the
// always-allow groups and paths have to keep answering before the delegated
// authorizer is consulted, or an unauthenticated health probe would start
// costing a review and system:masters would stop being short-circuited. The
// union is asserted in a test rather than assumed.
func (o *SpillwayServerOptions) applyAuthorization(config *genericapiserver.RecommendedConfig) error {
	options := o.RecommendedOptions.Authorization
	if options == nil {
		return nil
	}

	client, err := o.authorizationClient()
	if err != nil {
		return err
	}
	if client == nil {
		// No cluster to ask. Whatever the recommended options installed -- a deny
		// all authorizer, or the always-allow paths alone -- stands.
		return nil
	}

	built, err := buildAuthorizer(options, client)
	if err != nil {
		return err
	}
	config.Authorization.Authorizer = built
	return nil
}

// buildAuthorizer mirrors DelegatingAuthorizationOptions.toAuthorizer, which is
// unexported. Any failure is returned rather than dropped: a union missing one
// of its members would answer differently, and refusing to start beats starting
// with an authorizer that is not the one the flags describe.
func buildAuthorizer(options *genericoptions.DelegatingAuthorizationOptions, client kubernetes.Interface) (authorizer.Authorizer, error) {
	var authorizers []authorizer.Authorizer

	if len(options.AlwaysAllowGroups) > 0 {
		authorizers = append(authorizers, authorizerfactory.NewPrivilegedGroups(options.AlwaysAllowGroups...))
	}
	if len(options.AlwaysAllowPaths) > 0 {
		allowed, err := path.NewAuthorizer(options.AlwaysAllowPaths)
		if err != nil {
			return nil, fmt.Errorf("building the always-allow path authorizer: %w", err)
		}
		authorizers = append(authorizers, allowed)
	}

	delegated, err := (authorizerfactory.DelegatingAuthorizerConfig{
		SubjectAccessReviewClient: client.AuthorizationV1(),
		AllowCacheTTL:             options.AllowCacheTTL,
		DenyCacheTTL:              options.DenyCacheTTL,
		WebhookRetryBackoff:       options.WebhookRetryBackoff,
	}).New()
	if err != nil {
		return nil, fmt.Errorf("building the delegated authorizer: %w", err)
	}
	authorizers = append(authorizers, delegated)

	return union.New(authorizers...), nil
}
