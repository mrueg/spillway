// Package apiserver wires up the aggregated API server that fronts kcp.
package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"time"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	clientgocache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/mrueg/spillway/pkg/kcp"
)

// ExtraConfig holds the spillway specific configuration: how to reach the kcp
// workspace that stores the offloaded resources, and which API groups are
// served from it.
// WorkspaceConfig is one kcp workspace and the groups it backs.
type WorkspaceConfig struct {
	// Name identifies the workspace in logs, metrics and errors.
	Name string

	// Kubeconfig is the path to its credentials. Empty falls back to the usual
	// in-cluster and KUBECONFIG resolution.
	Kubeconfig string

	// APIGroups are the groups served from it.
	APIGroups *kcp.GroupMatcher

	// Backend tunes how requests to this workspace are timed out, retried and
	// cut off. One kcp being slow is a fact about that kcp, so it is settable
	// per workspace rather than by retuning every workspace at once.
	Backend BackendOptions

	// ImpersonateUsers forwards the caller's identity to this workspace, so its
	// own RBAC applies on top of the workload cluster's. Whether that is
	// possible depends on the identity spillway holds there, which is per
	// workspace too.
	ImpersonateUsers bool
}

type ExtraConfig struct {
	// Workspaces are the kcp workspaces spillway serves from, in the order
	// their configuration listed them: a group nobody names exactly is served
	// by the first workspace whose wildcard matches it.
	Workspaces []WorkspaceConfig

	// MirrorNamespaces creates a namespace in the workspace the first time
	// something is written into it, rather than requiring it in both places.
	MirrorNamespaces bool

	// CredentialReloadPeriod is how often each workspace's kubeconfig is
	// re-read, so a rotated token does not need a restart. Zero disables it.
	CredentialReloadPeriod time.Duration

	// ResyncPeriod is the backstop interval for re-examining the workspace,
	// behind the watch that normally triggers a refresh.
	ResyncPeriod time.Duration

	// ImpersonateUsers forwards the caller's identity to kcp, so the workspace's
	// RBAC applies on top of the workload cluster's.
	ImpersonateUsers bool

	// Backend tunes how requests to kcp are timed out, retried, and cut off
	// when kcp is failing.
	Backend BackendOptions

	// APIServices describes the APIServices spillway registers for the group
	// versions the workspace serves, when it is asked to register them at all.
	APIServices APIServiceOptions

	// LeaderElection decides whether one replica maintains those APIServices or
	// all of them do.
	LeaderElection LeaderElectionOptions

	// ReloadWorkspaces re-reads the workspace configuration, for the sources
	// that can change while spillway runs. Nil when there is nothing to
	// re-read, as when the single workspace came from flags.
	ReloadWorkspaces func() ([]WorkspaceConfig, error)

	// ClusterClientConfig reaches the workload cluster spillway is aggregated
	// into. It is carried here because completing the recommended config drops
	// it, and registering APIServices needs it.
	ClusterClientConfig *rest.Config
}

// Config is the full configuration of a spillway server.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// GroupHealthPath serves the per group status of the configured API groups:
// the path itself aggregates them, and <path>/<group> reports one. It is
// deliberately outside livez and readyz, which must reflect whether spillway
// itself can serve rather than whether kcp offers a particular group.
//
// Like the other health endpoints it is served without authorization; see
// NewSpillwayServerOptions. It reports only which of the configured groups the
// workspace serves, which is the same class of information as /healthz.
const GroupHealthPath = "/healthz/groups"

// Rate limits for the clients that read the workspace's API surface. Generous
// rather than unlimited: kcp's own priority and fairness is the real defence,
// but a bug here should not be able to hammer it without bound.
const (
	clientQPS   = 50
	clientBurst = 100
)

// Spillway serves API groups on behalf of the aggregation layer, backed by kcp
// rather than by the cluster's own etcd.
type Spillway struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig is a Config with defaults applied. It can only be produced by
// Complete, which is what makes the defaulting mandatory.
type CompletedConfig struct {
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data.
func (c *Config) Complete() CompletedConfig {
	completed := completedConfig{
		GenericConfig: c.GenericConfig.Complete(),
		ExtraConfig:   &c.ExtraConfig,
	}

	return CompletedConfig{&completed}
}

// New builds a spillway server from the completed configuration.
func (c CompletedConfig) New() (*Spillway, error) {
	genericServer, err := c.GenericConfig.New("spillway", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	kcp.RegisterMetrics()

	// The admission chain is already built against the workload cluster's
	// client and informers, so it reads that cluster's webhook configurations
	// and dials its webhook services. Nothing is copied into the cluster: a
	// webhook is called with an AdmissionReview, not with a stored object.
	//
	// The chain cannot say whether a webhook would act on a given resource, so
	// the configurations are consulted directly. It is shared: the webhooks are
	// the workload cluster's, whichever workspace stores the object.
	admissionInformers := c.GenericConfig.SharedInformerFactory.Admissionregistration().V1()
	matcher := &admissionMatcher{
		validating: admissionInformers.ValidatingWebhookConfigurations().Lister(),
		mutating:   admissionInformers.MutatingWebhookConfigurations().Lister(),
		bindings:   admissionInformers.ValidatingAdmissionPolicyBindings().Lister(),
		synced: []clientgocache.InformerSynced{
			admissionInformers.ValidatingWebhookConfigurations().Informer().HasSynced,
			admissionInformers.MutatingWebhookConfigurations().Informer().HasSynced,
			admissionInformers.ValidatingAdmissionPolicyBindings().Informer().HasSynced,
		},
	}

	routes := newRouter(nil)

	handler := &discoveryHandler{
		cache:      routes,
		owns:       routes.Owns,
		discovery:  genericServer.DiscoveryGroupManager,
		aggregated: genericServer.AggregatedDiscoveryGroupManager,
		proxy:      routes.proxyFor,
	}
	handler.install(genericServer.Handler.NonGoRestfulMux)

	openAPI := &openAPIHandler{
		cache:  routes,
		groups: routes.ServedGroups,
		// The same transports as the proxies: one connection pool, one circuit
		// breaker, one set of metrics per workspace covering everything
		// spillway asks of it.
		fetcher:    routes.fetcherFor,
		fetchers:   routes.fetchers,
		generation: routes.Generation,
	}
	openAPI.prepare()
	openAPI.install(genericServer.Handler.NonGoRestfulMux)

	if len(c.ExtraConfig.Workspaces) == 0 {
		return nil, fmt.Errorf("no kcp workspaces are configured; spillway would serve nothing")
	}

	manager := &workspaceManager{
		router:           routes,
		build:            func(configured WorkspaceConfig) (*workspace, error) { return c.newWorkspace(configured, matcher) },
		reload:           c.ExtraConfig.ReloadWorkspaces,
		resyncPeriod:     c.ExtraConfig.ResyncPeriod,
		credentialReload: c.ExtraConfig.CredentialReloadPeriod,
		watch: func(ctx context.Context, w *workspace) (<-chan struct{}, error) {
			return kcp.WatchCRDs(ctx, w.clientConfig(), w.groups)
		},
	}

	genericServer.AddPostStartHookOrDie("spillway-kcp-discovery",
		func(hookCtx genericapiserver.PostStartHookContext) error {
			onChange := handler.publish
			if c.ExtraConfig.APIServices.Register {
				registrar, err := newAPIServiceRegistrar(c.ExtraConfig.ClusterClientConfig, routes,
					c.ExtraConfig.APIServices)
				if err != nil {
					return err
				}

				// Registering before the hook returns means the APIServices for
				// what the workspaces serve exist by the time anything asks for
				// them. A failure is logged rather than fatal: spillway serves
				// its groups whether or not it managed to advertise them, and
				// the next refresh tries again.
				onChange = func() {
					handler.publish()
					if err := registrar.sync(hookCtx); err != nil {
						klog.FromContext(hookCtx).Error(err, "Reconciling APIServices")
					}
				}

				if c.ExtraConfig.LeaderElection.Enabled {
					// Reconciling on election as well as on change, because a
					// replica that has just taken over has missed everything
					// that happened while the previous one held the lease.
					leader, err := runLeaderElection(hookCtx, c.ExtraConfig.ClusterClientConfig,
						c.ExtraConfig.LeaderElection, func() { onChange() })
					if err != nil {
						return err
					}
					registrar.leader = leader
				}
			}
			manager.onChange = onChange

			if err := manager.start(hookCtx, c.ExtraConfig.Workspaces); err != nil {
				return err
			}
			onChange()

			// The configuration is re-read on the same interval the credentials
			// are, so adding a workspace is as undisruptive as rotating a token.
			go manager.run(hookCtx, c.ExtraConfig.CredentialReloadPeriod)
			return nil
		})

	// Readiness covers only whether spillway can answer at all. A group the
	// workspace does not serve is reported per group below rather than here:
	// failing readiness would pull the pod out of its Service endpoints and so
	// take down every other group with it, which is strictly worse than serving
	// the groups that are healthy.
	if err := genericServer.AddReadyzChecks(healthz.NamedCheck("kcp-discovery-synced",
		func(_ *http.Request) error {
			if !routes.HasSynced() {
				return fmt.Errorf("kcp discovery has not synced yet")
			}
			return nil
		})); err != nil {
		return nil, fmt.Errorf("registering the kcp discovery readiness check: %w", err)
	}

	// Per group checks are installed on their own path rather than through
	// AddHealthChecks, which despite its name registers against livez and readyz
	// as well -- a group the workspace does not serve would then fail liveness
	// and get the pod restarted.
	// Only groups named outright are checked. A wildcard that matches nothing is
	// a workspace that has not been given that kind of CRD, which is not a
	// fault; a named group that is missing is.
	var named []string
	for _, configured := range c.ExtraConfig.Workspaces {
		named = append(named, configured.APIGroups.Exact()...)
	}
	groupChecks := make([]healthz.HealthChecker, 0, len(named))
	for _, group := range named {
		group := group
		groupChecks = append(groupChecks, healthz.NamedCheck(group, func(_ *http.Request) error {
			if !routes.HasSynced() {
				return fmt.Errorf("kcp discovery has not synced yet")
			}
			if _, found := routes.Snapshot().Groups[group]; !found {
				return fmt.Errorf("no kcp workspace serves group %s", group)
			}
			// Discovery is answered from a cache that survives kcp being
			// unreachable, so a group can look healthy while every request for
			// its objects is being failed fast. The breaker is reported here
			// rather than in readyz for the same reason the rest of this is:
			// failing readiness removes the pod from its Service and takes the
			// other groups down with it, and every replica would do it at once.
			if serving := routes.servingFor(group); serving != nil && serving.proxy.backend.breaker.open() {
				return fmt.Errorf("the circuit to workspace %s is open: requests are being failed fast",
					serving.name)
			}
			return nil
		}))
	}
	if len(groupChecks) > 0 {
		healthz.InstallPathHandler(genericServer.Handler.NonGoRestfulMux, GroupHealthPath, groupChecks...)
	}

	return &Spillway{GenericAPIServer: genericServer}, nil
}

// newWorkspace assembles everything spillway needs for one kcp workspace: what
// it serves, how requests reach it, and the side channel admission and OpenAPI
// use.
func (c CompletedConfig) newWorkspace(configured WorkspaceConfig, matcher *admissionMatcher) (*workspace, error) {
	config, err := kcp.RestConfig(configured.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("workspace %s: %w", configured.Name, err)
	}

	// client-go throttles to 5 requests a second by default, which is a poor
	// fit here: one refresh costs a request per group version, and the CRD
	// informer relists on every reconnect. The proxy is unaffected either way,
	// since it uses its own transport rather than a REST client.
	throttled := rest.CopyConfig(config)
	throttled.QPS = clientQPS
	throttled.Burst = clientBurst

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(throttled)
	if err != nil {
		return nil, fmt.Errorf("building a discovery client for workspace %s: %w", configured.Name, err)
	}

	// The series exist from startup, so a workspace with a quiet breaker is
	// distinguishable from one that is not there at all.
	kcp.PublishWorkspace(configured.Name)

	built := &workspace{
		name:   configured.Name,
		config: throttled,
		groups: configured.APIGroups,
		cache:  kcp.NewResourceCache(configured.Name, discoveryClient, configured.APIGroups),
	}

	// An ownerReference may only name something in the same workspace: a group
	// served from a different one is as invisible to this workspace's garbage
	// collector as a group spillway does not serve at all.
	proxy, err := newResourceProxy(configured.Name, configured.Kubeconfig, config, configured.ImpersonateUsers,
		configured.Backend, built.cache.ServedGroups)
	if err != nil {
		return nil, fmt.Errorf("workspace %s: %w", configured.Name, err)
	}

	built.proxy = proxy
	built.backend = &backendClient{location: proxy.location, transport: proxy.transport}
	// The definitions are read through the same side channel everything else
	// uses, so a conversion question is bounded, counted and cut off with the
	// rest of what spillway asks of this workspace.
	conversion := &conversionPolicy{
		definitions: func(ctx context.Context) ([]byte, error) {
			return built.backend.fetchSpec(ctx, crdPath)
		},
		generation: built.cache.Generation,
	}
	proxy.admission = newAdmissionGate(c.GenericConfig.AdmissionControl, built.backend, matcher, conversion)
	if c.ExtraConfig.MirrorNamespaces {
		proxy.namespaces = newNamespaceMirror(built.backend)
	}

	return built, nil
}

// clientConfig is the throttled configuration the informers use.
func (w *workspace) clientConfig() *rest.Config {
	return w.config
}
