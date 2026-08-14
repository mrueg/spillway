// Package server assembles the command line surface of the spillway server.
package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	netutils "k8s.io/utils/net"

	"github.com/mrueg/spillway/pkg/apiserver"
	"github.com/mrueg/spillway/pkg/kcp"
	"github.com/mrueg/spillway/pkg/version"
)

// SpillwayServerOptions are the flags and derived state needed to run spillway.
type SpillwayServerOptions struct {
	RecommendedOptions *genericoptions.RecommendedOptions

	// KCPKubeconfig locates the kcp workspace that stores the offloaded resources.
	KCPKubeconfig string

	// WorkspacesFile configures more than one workspace, which flags cannot
	// express: the pairing of a kubeconfig with the groups it backs would
	// otherwise be positional.
	WorkspacesFile string

	// APIGroups are the API groups served on behalf of that workspace.
	APIGroups []string

	// ResyncPeriod is the backstop interval for re-examining the workspace.
	ResyncPeriod time.Duration

	// CredentialReloadPeriod is how often to re-read the kcp kubeconfigs.
	CredentialReloadPeriod time.Duration

	// MirrorNamespaces creates namespaces in the workspace on demand.
	MirrorNamespaces bool

	// ImpersonateUsers forwards the caller's identity to kcp.
	ImpersonateUsers bool

	// Backend tunes the connection to kcp on the proxy path.
	Backend apiserver.BackendOptions

	// MaxRequestsInFlight and MaxMutatingRequestsInFlight bound how many
	// requests spillway serves at once. The generic server fixes them at 400
	// and 200 and exposes no flag, which is a ceiling rather than a default for
	// something whose job is absorbing a cluster's read and write load.
	MaxRequestsInFlight         int
	MaxMutatingRequestsInFlight int

	// APIServices describes the APIServices spillway registers for itself.
	APIServices apiserver.APIServiceOptions

	// LeaderElection decides whether one replica maintains them or all do.
	LeaderElection apiserver.LeaderElectionOptions

	// AuthorizationQPS and AuthorizationBurst bound the rate at which spillway
	// sends SubjectAccessReviews to the cluster. See authorization.go for why
	// they exist at all.
	AuthorizationQPS   int
	AuthorizationBurst int

	StdOut io.Writer
	StdErr io.Writer
}

// The generic server's own in-flight defaults, restated because it offers no
// way to ask for them. Here they are a starting point rather than the ceiling
// they were.
const (
	defaultMaxRequestsInFlight         = 400
	defaultMaxMutatingRequestsInFlight = 200
)

// NewSpillwayServerOptions returns the default options.
func NewSpillwayServerOptions(out, errOut io.Writer) *SpillwayServerOptions {
	o := &SpillwayServerOptions{
		RecommendedOptions:          genericoptions.NewRecommendedOptions("", apiserver.Codecs.LegacyCodec()),
		ResyncPeriod:                10 * time.Minute,
		CredentialReloadPeriod:      time.Minute,
		MaxRequestsInFlight:         defaultMaxRequestsInFlight,
		MaxMutatingRequestsInFlight: defaultMaxMutatingRequestsInFlight,
		LeaderElection: apiserver.LeaderElectionOptions{
			Enabled:       true,
			Namespace:     apiserver.DefaultServiceNamespace(),
			Name:          "spillway-apiservices",
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
		},
		APIServices: apiserver.APIServiceOptions{
			ServiceName:          "spillway",
			ServiceNamespace:     apiserver.DefaultServiceNamespace(),
			ServicePort:          443,
			GroupPriorityMinimum: 1000,
			VersionPriority:      15,
		},
		AuthorizationQPS:   defaultAuthorizationQPS,
		AuthorizationBurst: defaultAuthorizationBurst,
		Backend: apiserver.BackendOptions{
			RequestTimeout:   30 * time.Second,
			Retries:          2,
			FailureThreshold: 5,
			CircuitCooldown:  10 * time.Second,
		},
		StdOut: out,
		StdErr: errOut,
	}

	// Spillway keeps no state of its own -- kcp is the store -- so the etcd
	// options are dropped rather than left half configured.
	o.RecommendedOptions.Etcd = nil

	// The generic health endpoints are exempt from authorization by default,
	// and the per group ones are the same kind of endpoint: a probe or an
	// operator debugging an APIService has no credentials to offer. The
	// trailing * covers <path>/<group> as well as the aggregate.
	o.RecommendedOptions.Authorization.WithAlwaysAllowPaths(apiserver.GroupHealthPath + "*")

	return o
}

// NewCommandStartSpillway builds the root command.
func NewCommandStartSpillway(ctx context.Context, defaults *SpillwayServerOptions) *cobra.Command {
	o := *defaults

	cmd := &cobra.Command{
		Use:   "spillway",
		Short: "Serve Kubernetes API groups from kcp instead of the cluster's own apiserver",
		Long: "Spillway is an aggregated (extension) API server. Register it with an APIService and " +
			"the API groups it owns are served out of a kcp workspace, diverting their read and write " +
			"load away from kube-apiserver and etcd.",
		SilenceUsage: true,
		RunE: func(c *cobra.Command, _ []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run(c.Context())
		},
	}
	cmd.SetContext(ctx)

	o.AddFlags(cmd.Flags())

	return cmd
}

// AddFlags registers the server flags on fs.
func (o *SpillwayServerOptions) AddFlags(fs *pflag.FlagSet) {
	o.RecommendedOptions.AddFlags(fs)

	fs.StringVar(&o.WorkspacesFile, "workspaces-file", o.WorkspacesFile,
		"Path to a YAML file listing the kcp workspaces to serve from and the API groups each one "+
			"backs, for serving more than one. Mutually exclusive with --kcp-kubeconfig and "+
			"--api-group, which are the single workspace form of the same thing.")
	fs.StringVar(&o.KCPKubeconfig, "kcp-kubeconfig", o.KCPKubeconfig,
		"Path to a kubeconfig pointing at the kcp workspace that backs the offloaded APIs. "+
			"Defaults to the standard in-cluster or KUBECONFIG resolution when unset.")
	fs.StringSliceVar(&o.APIGroups, "api-group", o.APIGroups,
		"An API group to serve from the kcp workspace, e.g. widgets.example.com, or a domain "+
			"wildcard such as *.example.com to serve every group under it that the workspace "+
			"has now or gains later. Every version the workspace serves for a matched group is "+
			"exposed. Repeat the flag for more.")
	fs.DurationVar(&o.ResyncPeriod, "kcp-resync-period", o.ResyncPeriod,
		"How often to re-examine the kcp workspace regardless of any change. API changes are "+
			"normally picked up from a watch on the workspace's CRDs; this is the backstop behind it.")
	fs.DurationVar(&o.CredentialReloadPeriod, "kcp-credential-reload-period", o.CredentialReloadPeriod,
		"How often to re-read each workspace's kubeconfig so a rotated token is picked up without a "+
			"restart, which would drop every proxied watch. Zero disables it. Only the credentials "+
			"are replaced: a kubeconfig that moves the workspace to another URL is reported and "+
			"otherwise ignored.")
	fs.DurationVar(&o.Backend.RequestTimeout, "kcp-request-timeout", o.Backend.RequestTimeout,
		"How long to wait for kcp to answer a proxied request. Watches are exempt from the deadline, "+
			"but not from the wait for their first response header.")
	fs.IntVar(&o.Backend.Retries, "kcp-retries", o.Backend.Retries,
		"How many times to retry a read after a connection level failure. Requests that change "+
			"state are never retried.")
	fs.IntVar(&o.Backend.FailureThreshold, "kcp-failure-threshold", o.Backend.FailureThreshold,
		"Consecutive kcp failures before the circuit opens and requests fail fast instead of "+
			"queueing against an unresponsive kcp.")
	fs.DurationVar(&o.Backend.CircuitCooldown, "kcp-circuit-cooldown", o.Backend.CircuitCooldown,
		"How long the circuit stays open before a single request is let through to test kcp.")
	fs.BoolVar(&o.LeaderElection.Enabled, "leader-elect", o.LeaderElection.Enabled,
		"Have one replica maintain the registered APIServices rather than all of them. Serving needs "+
			"no leader and is unaffected; this is about who writes. Ignored unless "+
			"--register-apiservices is set.")
	fs.StringVar(&o.LeaderElection.Name, "leader-elect-resource-name", o.LeaderElection.Name,
		"Name of the Lease used to elect that replica.")
	fs.StringVar(&o.LeaderElection.Namespace, "leader-elect-resource-namespace", o.LeaderElection.Namespace,
		"Namespace of that Lease. Defaults to the namespace spillway is running in.")
	fs.DurationVar(&o.LeaderElection.LeaseDuration, "leader-elect-lease-duration", o.LeaderElection.LeaseDuration,
		"How long a replica that has stopped renewing holds the lease before another may take it.")
	fs.DurationVar(&o.LeaderElection.RenewDeadline, "leader-elect-renew-deadline", o.LeaderElection.RenewDeadline,
		"How long the holder has to renew before giving up leadership.")
	fs.DurationVar(&o.LeaderElection.RetryPeriod, "leader-elect-retry-period", o.LeaderElection.RetryPeriod,
		"How often to retry acquiring or renewing the lease.")
	fs.IntVar(&o.MaxRequestsInFlight, "max-requests-inflight", o.MaxRequestsInFlight,
		"How many non-mutating requests spillway serves concurrently before shedding load with a 429. "+
			"Long-running requests, watches among them, are exempt. Zero means no limit.")
	fs.IntVar(&o.MaxMutatingRequestsInFlight, "max-mutating-requests-inflight", o.MaxMutatingRequestsInFlight,
		"The same, for requests that change state. Zero means no limit.")
	fs.BoolVar(&o.APIServices.Register, "register-apiservices", o.APIServices.Register,
		"Create and maintain an APIService for every group version the kcp workspace serves, instead "+
			"of declaring them by hand. Only APIServices spillway created are ever updated or removed; "+
			"one written by hand is left alone. Requires permission to write apiservices.")
	fs.StringVar(&o.APIServices.ServiceName, "apiservice-service-name", o.APIServices.ServiceName,
		"Name of the Service the aggregation layer should reach spillway through.")
	fs.StringVar(&o.APIServices.ServiceNamespace, "apiservice-service-namespace", o.APIServices.ServiceNamespace,
		"Namespace of that Service. Defaults to the namespace spillway is running in.")
	fs.Int32Var(&o.APIServices.ServicePort, "apiservice-service-port", o.APIServices.ServicePort,
		"Port of that Service.")
	fs.StringVar(&o.APIServices.CABundleFile, "apiservice-ca-bundle-file", o.APIServices.CABundleFile,
		"PEM bundle of the authority that signed spillway's serving certificate, for the aggregation "+
			"layer to verify it against. Re-read on every refresh, so a rotation is picked up.")
	fs.BoolVar(&o.APIServices.InsecureSkipTLSVerify, "apiservice-insecure-skip-tls-verify",
		o.APIServices.InsecureSkipTLSVerify,
		"Register APIServices that do not verify spillway's serving certificate. What a self-signed "+
			"startup certificate needs, and not something to run in production.")
	fs.Int32Var(&o.APIServices.GroupPriorityMinimum, "apiservice-group-priority-minimum",
		o.APIServices.GroupPriorityMinimum, "groupPriorityMinimum for the registered APIServices.")
	fs.Int32Var(&o.APIServices.VersionPriority, "apiservice-version-priority",
		o.APIServices.VersionPriority, "versionPriority for the registered APIServices.")
	fs.IntVar(&o.AuthorizationQPS, "authorization-qps", o.AuthorizationQPS,
		"How many SubjectAccessReviews per second spillway may send to the cluster. One is sent "+
			"per request whose subject, verb, resource and object name the authorizer has not "+
			"cached, so throughput on distinct objects is capped by this. Every review is load on "+
			"the apiserver spillway is unloading.")
	fs.IntVar(&o.AuthorizationBurst, "authorization-burst", o.AuthorizationBurst,
		"How far the SubjectAccessReview rate may exceed --authorization-qps in a burst.")
	fs.BoolVar(&o.MirrorNamespaces, "mirror-namespaces", o.MirrorNamespaces,
		"Create a namespace in the kcp workspace the first time an object is written into it, "+
			"instead of requiring it to exist in both places. Only namespaces that are used are "+
			"created, they are labelled as spillway's, and none is ever deleted.")
	fs.BoolVar(&o.ImpersonateUsers, "kcp-impersonate-users", o.ImpersonateUsers,
		"Forward the caller's identity to kcp so that the workspace's RBAC applies on top of this "+
			"cluster's. Requires spillway's kcp identity to hold impersonate permissions, and the "+
			"workspace to carry RBAC for this cluster's users. When false, requests reach kcp as "+
			"spillway itself and this cluster's RBAC is the only authorization check.")
}

// Complete fills in fields that are derived from other flags.
func (o *SpillwayServerOptions) Complete() error {
	// A group repeated on the command line would register its handlers twice,
	// which the mux reports as a duplicate path registration. Sorting as well
	// keeps the served discovery order stable across restarts.
	if len(o.APIGroups) > 0 {
		o.APIGroups = sets.List(sets.New(o.APIGroups...))
	}
	return nil
}

// Validate checks the options for internal consistency.
func (o *SpillwayServerOptions) Validate() error {
	errs := o.RecommendedOptions.Validate()

	switch {
	case o.WorkspacesFile != "" && (len(o.APIGroups) > 0 || o.KCPKubeconfig != ""):
		errs = append(errs, fmt.Errorf("--workspaces-file cannot be combined with --kcp-kubeconfig or "+
			"--api-group; the file lists the kubeconfig and the groups for every workspace"))
	case o.WorkspacesFile != "":
		if _, err := loadWorkspaces(o.WorkspacesFile, o.workspaceDefaults()); err != nil {
			errs = append(errs, fmt.Errorf("--workspaces-file: %w", err))
		}
	case len(o.APIGroups) == 0:
		errs = append(errs, fmt.Errorf("at least one --api-group is required; spillway would serve nothing otherwise"))
	default:
		if _, err := kcp.ParseGroupMatcher(o.APIGroups); err != nil {
			errs = append(errs, fmt.Errorf("--api-group: %w", err))
		}
	}
	if o.CredentialReloadPeriod < 0 {
		errs = append(errs, fmt.Errorf("--kcp-credential-reload-period must not be negative, got %s",
			o.CredentialReloadPeriod))
	}
	if o.ResyncPeriod <= 0 {
		errs = append(errs, fmt.Errorf("--kcp-resync-period must be positive, got %s", o.ResyncPeriod))
	}
	if o.Backend.RequestTimeout <= 0 {
		errs = append(errs, fmt.Errorf("--kcp-request-timeout must be positive, got %s", o.Backend.RequestTimeout))
	}
	if o.Backend.Retries < 0 {
		errs = append(errs, fmt.Errorf("--kcp-retries must not be negative, got %d", o.Backend.Retries))
	}
	if o.Backend.FailureThreshold < 1 {
		errs = append(errs, fmt.Errorf("--kcp-failure-threshold must be at least 1, got %d", o.Backend.FailureThreshold))
	}
	if o.Backend.CircuitCooldown <= 0 {
		errs = append(errs, fmt.Errorf("--kcp-circuit-cooldown must be positive, got %s", o.Backend.CircuitCooldown))
	}
	if o.MaxRequestsInFlight < 0 {
		errs = append(errs, fmt.Errorf("--max-requests-inflight must not be negative, got %d", o.MaxRequestsInFlight))
	}
	if o.MaxMutatingRequestsInFlight < 0 {
		errs = append(errs, fmt.Errorf("--max-mutating-requests-inflight must not be negative, got %d",
			o.MaxMutatingRequestsInFlight))
	}

	errs = append(errs, o.APIServices.Validate()...)
	if o.APIServices.Register {
		errs = append(errs, o.LeaderElection.Validate()...)
	}
	if o.AuthorizationQPS <= 0 {
		errs = append(errs, fmt.Errorf("--authorization-qps must be positive, got %d", o.AuthorizationQPS))
	}
	if o.AuthorizationBurst < o.AuthorizationQPS {
		errs = append(errs, fmt.Errorf("--authorization-burst (%d) must be at least --authorization-qps (%d)",
			o.AuthorizationBurst, o.AuthorizationQPS))
	}

	return utilerrors.NewAggregate(errs)
}

// Config turns the options into a server configuration.
func (o *SpillwayServerOptions) Config() (*apiserver.Config, error) {
	// The aggregation layer talks to spillway over TLS, so it needs serving
	// certs even in a local development run.
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts(
		"localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")},
	); err != nil {
		return nil, fmt.Errorf("creating self-signed certificates: %w", err)
	}

	workspaces, err := o.workspaces()
	if err != nil {
		return nil, err
	}

	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)
	serverConfig.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString(version.APIVersion(), "", "")

	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return nil, err
	}

	// Both after ApplyTo, which does not touch either but would overwrite the
	// authorizer.
	serverConfig.MaxRequestsInFlight = o.MaxRequestsInFlight
	serverConfig.MaxMutatingRequestsInFlight = o.MaxMutatingRequestsInFlight

	// After ApplyTo, so this replaces the authorizer it installed rather than
	// being replaced by it.
	if err := o.applyAuthorization(serverConfig); err != nil {
		return nil, err
	}

	return &apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig:   o.extraConfig(workspaces, serverConfig.ClientConfig),
	}, nil
}

// extraConfig maps the options onto what the server actually reads.
//
// It is its own function because it is a list of assignments, and a missing one
// is invisible: the flag parses, it validates, it appears in --help, and it does
// nothing. That has happened three times in this file. A test over this covers
// the mapping itself, which is the part that keeps going wrong.
func (o *SpillwayServerOptions) extraConfig(workspaces []apiserver.WorkspaceConfig,
	cluster *rest.Config) apiserver.ExtraConfig {
	return apiserver.ExtraConfig{
		Workspaces:             workspaces,
		ReloadWorkspaces:       o.reloadWorkspaces(),
		ClusterClientConfig:    cluster,
		APIServices:            o.APIServices,
		LeaderElection:         o.LeaderElection,
		ResyncPeriod:           o.ResyncPeriod,
		CredentialReloadPeriod: o.CredentialReloadPeriod,
		ImpersonateUsers:       o.ImpersonateUsers,
		MirrorNamespaces:       o.MirrorNamespaces,
		Backend:                o.Backend,
	}
}

// workspaces resolves what spillway serves from where: the file when one was
// given, and otherwise the single workspace the flags describe.
func (o *SpillwayServerOptions) workspaces() ([]apiserver.WorkspaceConfig, error) {
	if o.WorkspacesFile != "" {
		return loadWorkspaces(o.WorkspacesFile, o.workspaceDefaults())
	}

	groups, err := kcp.ParseGroupMatcher(o.APIGroups)
	if err != nil {
		return nil, fmt.Errorf("--api-group: %w", err)
	}

	single := o.workspaceDefaults()
	single.Name = "default"
	single.Kubeconfig = o.KCPKubeconfig
	single.APIGroups = groups
	return []apiserver.WorkspaceConfig{single}, nil
}

// workspaceDefaults is what a workspace gets when it does not say otherwise:
// the flags, which is the whole configuration when there is only one.
func (o *SpillwayServerOptions) workspaceDefaults() apiserver.WorkspaceConfig {
	return apiserver.WorkspaceConfig{
		Backend:          o.Backend,
		ImpersonateUsers: o.ImpersonateUsers,
	}
}

// reloadWorkspaces returns a way to re-read the configuration, for the sources
// that can change while spillway runs. The flags cannot: they are the process's
// arguments, and changing them is a restart by definition.
func (o *SpillwayServerOptions) reloadWorkspaces() func() ([]apiserver.WorkspaceConfig, error) {
	if o.WorkspacesFile == "" {
		return nil
	}
	path, defaults := o.WorkspacesFile, o.workspaceDefaults()
	return func() ([]apiserver.WorkspaceConfig, error) { return loadWorkspaces(path, defaults) }
}

// Run starts the server and blocks until ctx is cancelled.
func (o *SpillwayServerOptions) Run(ctx context.Context) error {
	config, err := o.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New()
	if err != nil {
		return err
	}

	server.GenericAPIServer.AddPostStartHookOrDie("spillway-start-informers",
		func(hookCtx genericapiserver.PostStartHookContext) error {
			config.GenericConfig.SharedInformerFactory.Start(hookCtx.Done())
			return nil
		})

	return server.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
