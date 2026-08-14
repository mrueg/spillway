package apiserver

import (
	"context"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	"k8s.io/utils/ptr"

	"github.com/mrueg/spillway/pkg/kcp"
)

// managedByLabel marks the APIServices spillway created, and is what separates
// the ones it may remove from the ones an operator declared.
const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "spillway"
)

// inClusterNamespaceFile is where a pod's own namespace is readable, which is a
// better default for the service reference than any string could be.
const inClusterNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// APIServiceOptions describes the APIServices spillway registers for itself.
//
// Without this, every group version has to be declared by hand and kept in step
// with what the workspace serves. That is two sources of truth for the same
// fact, and the one written by hand is the one that goes stale: a manifest
// naming v1alpha1 says nothing about the v1beta1 the workspace started serving
// yesterday, and nothing about a version it stopped serving -- which leaves an
// APIService that spillway will fail the availability probe for, degrading
// discovery for the whole cluster.
type APIServiceOptions struct {
	// Register turns the whole thing on. Off by default: it needs permission to
	// write APIServices, which a deployment has to grant.
	Register bool

	// ServiceName, ServiceNamespace and ServicePort locate spillway for the
	// aggregation layer.
	ServiceName      string
	ServiceNamespace string
	ServicePort      int32

	// CABundleFile holds the certificate authority that signed spillway's
	// serving certificate. Read on every sync so a rotation is picked up.
	CABundleFile string
	// InsecureSkipTLSVerify registers APIServices that do not verify spillway's
	// certificate, which is what a self-signed startup certificate needs.
	InsecureSkipTLSVerify bool

	GroupPriorityMinimum int32
	VersionPriority      int32
}

// DefaultServiceNamespace returns the namespace spillway is running in, when it
// can tell.
func DefaultServiceNamespace() string {
	namespace, err := os.ReadFile(inClusterNamespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(namespace))
}

// Validate checks the options, which only matters when registration is on.
func (o APIServiceOptions) Validate() []error {
	if !o.Register {
		return nil
	}

	var errs []error
	if o.ServiceName == "" {
		errs = append(errs, fmt.Errorf("--apiservice-service-name is required when --register-apiservices is set"))
	}
	if o.ServiceNamespace == "" {
		errs = append(errs, fmt.Errorf("--apiservice-service-namespace is required when --register-apiservices is set "+
			"and spillway cannot read its own namespace"))
	}
	if o.ServicePort <= 0 {
		errs = append(errs, fmt.Errorf("--apiservice-service-port must be positive, got %d", o.ServicePort))
	}
	// Registering an APIService that neither verifies the certificate nor knows
	// who signed it is not something to guess at: one of the two has to be said
	// out loud.
	if o.CABundleFile == "" && !o.InsecureSkipTLSVerify {
		errs = append(errs, fmt.Errorf("--apiservice-ca-bundle-file or --apiservice-insecure-skip-tls-verify "+
			"is required when --register-apiservices is set, so that the aggregation layer knows whether "+
			"to verify spillway's serving certificate"))
	}
	if o.CABundleFile != "" && o.InsecureSkipTLSVerify {
		errs = append(errs, fmt.Errorf("--apiservice-ca-bundle-file and --apiservice-insecure-skip-tls-verify "+
			"are mutually exclusive"))
	}
	return errs
}

// apiServiceRegistrar keeps the cluster's APIServices matching the group
// versions the workspace actually serves.
// registrarSource is what the registrar needs of the workspaces: what they
// serve, and what they are configured to serve.
type registrarSource interface {
	Snapshot() *kcp.Snapshot
	Owns(group string) bool
	HasSynced() bool
}

type apiServiceRegistrar struct {
	client  aggregatorclient.Interface
	cache   registrarSource
	options APIServiceOptions

	// cluster reports what the workload cluster already serves itself, which
	// spillway must not put itself in front of.
	cluster *clusterAPIs

	// leader reports whether this replica is the one that writes.
	leader *leadership

	// warned remembers which group versions have already been reported as
	// belonging to the cluster, so a refusal is logged once rather than on
	// every refresh forever.
	warned sets.Set[schema.GroupVersion]
}

// newAPIServiceRegistrar builds a registrar over the workload cluster's own
// client -- the same one admission uses, so registration needs no second
// kubeconfig.
func newAPIServiceRegistrar(config *rest.Config, cache registrarSource,
	options APIServiceOptions) (*apiServiceRegistrar, error) {
	if config == nil {
		return nil, fmt.Errorf("registering APIServices needs a connection to the workload cluster; " +
			"pass --kubeconfig or run spillway in the cluster")
	}

	client, err := aggregatorclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building the APIService client: %w", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building the discovery client for the workload cluster: %w", err)
	}

	return &apiServiceRegistrar{
		client:  client,
		cache:   cache,
		options: options,
		cluster: &clusterAPIs{
			discovery:  discoveryClient,
			aggregator: client,
			service: apiregistrationv1.ServiceReference{
				Name:      options.ServiceName,
				Namespace: options.ServiceNamespace,
			},
		},
		warned: sets.New[schema.GroupVersion](),
	}, nil
}

// sync reconciles every APIService spillway owns.
//
// It is deliberately conservative about what it owns. An APIService spillway
// created carries a label, and only those are ever updated or removed; one an
// operator wrote by hand is left exactly as it is, because deleting a declared
// object out from under whoever declared it is not spillway's call to make.
func (r *apiServiceRegistrar) sync(ctx context.Context) error {
	managed, err := r.reconcile(ctx)
	if err != nil {
		kcp.ObserveAPIServiceSync("error", managed)
		return err
	}
	kcp.ObserveAPIServiceSync("success", managed)
	return nil
}

// reconcile does the work and reports how many APIServices spillway is
// maintaining afterwards.
func (r *apiServiceRegistrar) reconcile(ctx context.Context) (int, error) {
	// Another replica is doing the writing. Its answers are the same as this
	// one's would be, from the same configuration.
	if !r.leader.leads() {
		return 0, nil
	}

	// A cache that has never synced cannot distinguish "the workspace serves
	// nothing" from "the workspace has not been read yet", and acting on the
	// second would withdraw every APIService spillway has.
	if !r.cache.HasSynced() {
		return 0, nil
	}

	caBundle, err := r.caBundle()
	if err != nil {
		return 0, err
	}

	// Read before anything is written, and fatal to the round if it fails: not
	// knowing what the cluster serves is not a licence to register over it.
	foreign, err := r.cluster.foreign(ctx)
	if err != nil {
		return 0, fmt.Errorf("checking what the cluster already serves: %w", err)
	}

	wanted := map[string]*apiregistrationv1.APIService{}
	for gv := range r.cache.Snapshot().Resources {
		if foreign.Has(gv) {
			r.warnForeign(ctx, gv)
			continue
		}
		desired := r.desired(gv, caBundle)
		wanted[desired.Name] = desired
	}

	existing, err := r.client.ApiregistrationV1().APIServices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("listing APIServices: %w", err)
	}

	var errs []error
	// Only what spillway actually maintains is counted: an APIService somebody
	// else declared is served but not managed, and reporting it here would
	// overstate what the registrar is responsible for.
	mine := 0
	present := sets.New[string]()
	for i := range existing.Items {
		current := &existing.Items[i]
		present.Insert(current.Name)

		desired, want := wanted[current.Name]
		managed := current.Labels[managedByLabel] == managedByValue

		switch {
		case want && !managed:
			// Somebody else declared this group version. Spillway serves it
			// either way; it just does not own the registration.
			klog.FromContext(ctx).V(4).Info("Leaving an APIService alone: it is not labelled as spillway's",
				"apiservice", current.Name)
		case want && managed:
			mine++
			if updated, changed := merge(current, desired); changed {
				if _, err := r.client.ApiregistrationV1().APIServices().Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
					errs = append(errs, fmt.Errorf("updating APIService %s: %w", current.Name, err))
					continue
				}
				klog.FromContext(ctx).V(2).Info("Updated an APIService", "apiservice", current.Name)
			}
		case !want && managed && r.owns(current):
			// The workspace stopped serving this group version. Left in place it
			// would fail the aggregation layer's availability probe, which is a
			// cluster wide symptom for a workspace level change.
			if err := r.client.ApiregistrationV1().APIServices().Delete(ctx, current.Name, metav1.DeleteOptions{}); err != nil &&
				!apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("deleting APIService %s: %w", current.Name, err))
				continue
			}
			klog.FromContext(ctx).V(2).Info("Withdrew an APIService: the workspace no longer serves it",
				"apiservice", current.Name)
		}
	}

	for name, desired := range wanted {
		if present.Has(name) {
			continue
		}
		if _, err := r.client.ApiregistrationV1().APIServices().Create(ctx, desired, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			errs = append(errs, fmt.Errorf("creating APIService %s: %w", name, err))
			continue
		}
		mine++
		klog.FromContext(ctx).V(2).Info("Registered an APIService", "apiservice", name)
	}

	return mine, utilerrors.NewAggregate(errs)
}

// warnForeign reports a group version spillway will not register because the
// cluster serves it already. Once per group version: it is a standing condition,
// not an event, and repeating it every refresh would bury everything else.
func (r *apiServiceRegistrar) warnForeign(ctx context.Context, gv schema.GroupVersion) {
	if r.warned.Has(gv) {
		return
	}
	r.warned.Insert(gv)

	klog.FromContext(ctx).Info("Refusing to register an APIService: the cluster serves this group "+
		"version itself, and registering would put spillway in front of it",
		"groupVersion", gv.String(),
		"consequence", "the workspace's objects in this group are not served through this cluster")
}

// owns reports whether a labelled APIService is one of spillway's groups, so
// that a label copied onto something unrelated cannot make spillway delete it.
// It asks where the APIService points, not what group it names. Both are ways
// of saying "this is mine", and the group is the wrong one: a workspace removed
// from the configuration takes its groups out of it too, so a registration that
// has to be withdrawn is precisely one whose group is no longer owned. Pointing
// at spillway's own Service cannot be spoofed by a label copied onto someone
// else's APIService, which is what the guard was for.
func (r *apiServiceRegistrar) owns(service *apiregistrationv1.APIService) bool {
	if service.Spec.Service == nil {
		return false
	}
	return service.Spec.Service.Name == r.options.ServiceName &&
		service.Spec.Service.Namespace == r.options.ServiceNamespace
}

func (r *apiServiceRegistrar) caBundle() ([]byte, error) {
	if r.options.CABundleFile == "" {
		return nil, nil
	}
	bundle, err := os.ReadFile(r.options.CABundleFile)
	if err != nil {
		return nil, fmt.Errorf("reading the APIService CA bundle: %w", err)
	}
	return bundle, nil
}

func (r *apiServiceRegistrar) desired(gv schema.GroupVersion, caBundle []byte) *apiregistrationv1.APIService {
	return &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name:   gv.Version + "." + gv.Group,
			Labels: map[string]string{managedByLabel: managedByValue},
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   gv.Group,
			Version: gv.Version,
			Service: &apiregistrationv1.ServiceReference{
				Name:      r.options.ServiceName,
				Namespace: r.options.ServiceNamespace,
				Port:      ptr.To(r.options.ServicePort),
			},
			CABundle:              caBundle,
			InsecureSkipTLSVerify: r.options.InsecureSkipTLSVerify,
			GroupPriorityMinimum:  r.options.GroupPriorityMinimum,
			VersionPriority:       r.options.VersionPriority,
		},
	}
}

// merge returns the object to write and whether anything needed writing. Only
// the fields spillway sets are compared, so an annotation somebody added stays.
func merge(current, desired *apiregistrationv1.APIService) (*apiregistrationv1.APIService, bool) {
	if equality.Semantic.DeepEqual(current.Spec, desired.Spec) &&
		current.Labels[managedByLabel] == managedByValue {
		return current, false
	}

	updated := current.DeepCopy()
	updated.Spec = desired.Spec
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	updated.Labels[managedByLabel] = managedByValue
	return updated, true
}
