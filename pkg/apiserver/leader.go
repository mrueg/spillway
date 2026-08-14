package apiserver

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// LeaderElectionOptions decides whether one replica or all of them maintain the
// APIServices.
//
// Serving needs no leader: every replica answers requests, and that is the
// point of having more than one. Writing does. The registrations are the same
// from every replica and converge whoever writes them, so this is not a
// correctness fix -- it is that N replicas reconciling the same objects means N
// times the writes, N times the conflicts to retry, and a log in which the same
// change appears N times.
type LeaderElectionOptions struct {
	// Enabled turns it on. Off means every replica registers, which is what
	// happens with a single replica anyway.
	Enabled bool

	// Namespace and Name locate the Lease. The namespace defaults to the one
	// spillway is running in.
	Namespace string
	Name      string

	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// Validate checks the durations relate to each other the way the election
// requires: a lease has to outlast the deadline for renewing it, which has to
// outlast the interval between attempts.
func (o LeaderElectionOptions) Validate() []error {
	if !o.Enabled {
		return nil
	}

	var errs []error
	if o.Name == "" {
		errs = append(errs, fmt.Errorf("--leader-elect-resource-name is required when --leader-elect is set"))
	}
	if o.Namespace == "" {
		errs = append(errs, fmt.Errorf("--leader-elect-resource-namespace is required when --leader-elect "+
			"is set and spillway cannot read its own namespace"))
	}
	if o.LeaseDuration <= o.RenewDeadline {
		errs = append(errs, fmt.Errorf("--leader-elect-lease-duration (%s) must be longer than "+
			"--leader-elect-renew-deadline (%s)", o.LeaseDuration, o.RenewDeadline))
	}
	if o.RenewDeadline <= o.RetryPeriod {
		errs = append(errs, fmt.Errorf("--leader-elect-renew-deadline (%s) must be longer than "+
			"--leader-elect-retry-period (%s)", o.RenewDeadline, o.RetryPeriod))
	}
	return errs
}

// leadership reports whether this replica is the one that writes.
//
// A nil leadership leads: that is the single replica case, and the case where
// election was not asked for, both of which want every replica to register.
type leadership struct {
	leading atomic.Bool
}

func (l *leadership) leads() bool {
	if l == nil {
		return true
	}
	return l.leading.Load()
}

// runLeaderElection blocks until ctx is done, holding or contending for the
// lease and reporting through the returned leadership.
//
// Losing the lease does not stop spillway. It stops it writing: the APIServices
// it registered stay exactly as they are, and whichever replica takes over
// reconciles them from the same configuration to the same values. A registrar
// that kept writing after losing the lease would be the one thing this exists to
// prevent.
func runLeaderElection(ctx context.Context, config *rest.Config, options LeaderElectionOptions,
	onElected func()) (*leadership, error) {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building the client for leader election: %w", err)
	}

	identity, err := os.Hostname()
	if err != nil || identity == "" {
		return nil, fmt.Errorf("reading this replica's identity for leader election: %w", err)
	}

	state := &leadership{}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: options.Name, Namespace: options.Namespace},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: options.LeaseDuration,
		RenewDeadline: options.RenewDeadline,
		RetryPeriod:   options.RetryPeriod,
		// Spillway keeps serving either way, so releasing on cancel is a
		// courtesy to the next replica rather than a safety measure: it hands
		// the lease over at shutdown instead of making everyone wait it out.
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {
				state.leading.Store(true)
				klog.FromContext(ctx).Info("Elected to maintain the APIServices", "identity", identity)
				if onElected != nil {
					onElected()
				}
			},
			OnStoppedLeading: func() {
				state.leading.Store(false)
				klog.FromContext(ctx).Info("No longer maintaining the APIServices", "identity", identity)
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("configuring leader election: %w", err)
	}

	go elector.Run(ctx)
	return state, nil
}
