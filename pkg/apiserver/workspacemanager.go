package apiserver

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	"github.com/mrueg/spillway/pkg/kcp"
)

// workspaceManager keeps the running workspaces matching what the configuration
// asks for, and can be asked again while spillway is serving.
//
// Rotating a token stopped needing a restart; adding a workspace still did,
// which is the more disruptive of the two. A restart drops every watch spillway
// is proxying and re-syncs discovery for all the workspaces that did not
// change, in order to pick up one that did.
//
// A workspace is identified by name. One whose kubeconfig or groups changed is
// replaced rather than mutated: its cache, its proxy and its credentials were
// all built from what it used to be.
type workspaceManager struct {
	router *router

	// build makes a workspace from its configuration. It is a function so the
	// manager does not need the whole server configuration.
	build func(WorkspaceConfig) (*workspace, error)

	// reload re-reads the configuration. Nil when there is nothing to re-read,
	// as when the single workspace came from flags.
	reload func() ([]WorkspaceConfig, error)

	resyncPeriod     time.Duration
	credentialReload time.Duration

	// onChange publishes discovery and reconciles the APIServices, and is
	// called whenever the set of workspaces changes as well as on every
	// refresh, so a workspace appearing or disappearing is advertised at once.
	onChange func()

	// watch starts the CRD watch for a workspace. Injected for tests, which
	// have no kcp to watch.
	watch func(ctx context.Context, w *workspace) (<-chan struct{}, error)
}

// fingerprint is what makes two configurations of a workspace the same one.
//
// Everything the workspace was built from is in it. A changed timeout or
// circuit threshold has to rebuild the workspace, because those were baked into
// its transport when it was made; leaving them out would make the setting
// re-readable in name only.
func fingerprint(configured WorkspaceConfig) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%v\x00%s\x00%d\x00%d\x00%s",
		configured.Name, configured.Kubeconfig, configured.APIGroups.String(),
		configured.ImpersonateUsers, configured.Backend.RequestTimeout, configured.Backend.Retries,
		configured.Backend.FailureThreshold, configured.Backend.CircuitCooldown)
}

// start brings up the workspaces the configuration describes. A failure here is
// fatal: spillway starting without a workspace it was told to serve would
// advertise nothing for those groups and look healthy doing it.
func (m *workspaceManager) start(ctx context.Context, configured []WorkspaceConfig) error {
	running := make([]*workspace, 0, len(configured))
	for _, entry := range configured {
		built, err := m.launch(ctx, entry)
		if err != nil {
			return err
		}
		running = append(running, built)
	}

	m.router.replace(running)
	return nil
}

// launch builds a workspace, reads it once, and starts everything that keeps it
// current.
func (m *workspaceManager) launch(ctx context.Context, configured WorkspaceConfig) (*workspace, error) {
	built, err := m.build(configured)
	if err != nil {
		return nil, err
	}
	built.fingerprint = fingerprint(configured)

	// Read it before it is announced, so it is never advertised as a workspace
	// that serves nothing.
	if err := built.cache.Refresh(); err != nil {
		return nil, fmt.Errorf("initial kcp discovery sync for workspace %s: %w", built.name, err)
	}

	scoped, cancel := context.WithCancel(ctx)
	built.cancel = cancel

	changed, err := m.watch(scoped, built)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("watching CustomResourceDefinitions in workspace %s: %w", built.name, err)
	}

	go built.cache.Run(scoped, m.resyncPeriod, changed, m.onChange)
	go built.proxy.credentials.run(scoped, m.credentialReload, built.name)

	return built, nil
}

// run re-reads the configuration until ctx is done.
func (m *workspaceManager) run(ctx context.Context, period time.Duration) {
	if m.reload == nil || period <= 0 {
		return
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.reconcile(ctx); err != nil {
				klog.FromContext(ctx).Error(err, "Re-reading the workspaces configuration")
				kcp.ObserveWorkspaceReload("error")
			}
		}
	}
}

// reconcile applies the configuration as it now reads.
//
// Removals are applied whatever else happens: a workspace taken out of the
// configuration is one an operator wants spillway to stop serving, and leaving
// it running because some unrelated addition failed to build would be the
// opposite of what they asked for. An addition that fails is logged and left
// for the next tick, one workspace at a time, so a single bad entry does not
// hold back the rest.
func (m *workspaceManager) reconcile(ctx context.Context) error {
	desired, err := m.reload()
	if err != nil {
		return err
	}

	log := klog.FromContext(ctx)
	current := m.router.snapshot()

	byName := make(map[string]*workspace, len(current))
	for _, running := range current {
		byName[running.name] = running
	}

	next := make([]*workspace, 0, len(desired))
	keep := make(map[string]bool, len(desired))
	changes := 0

	for _, entry := range desired {
		running, found := byName[entry.Name]
		if found && running.fingerprint == fingerprint(entry) {
			keep[entry.Name] = true
			next = append(next, running)
			continue
		}

		built, err := m.launch(ctx, entry)
		if err != nil {
			log.Error(err, "Bringing up a workspace from the reloaded configuration", "workspace", entry.Name)
			kcp.ObserveWorkspaceReload("error")
			// The old one, if there is one, keeps serving rather than being
			// torn down for a replacement that does not work.
			if found {
				keep[entry.Name] = true
				next = append(next, running)
			}
			continue
		}

		if found {
			log.Info("Replaced a workspace", "workspace", entry.Name)
		} else {
			log.Info("Added a workspace", "workspace", entry.Name)
		}
		next = append(next, built)
		changes++
	}

	for _, running := range current {
		if keep[running.name] {
			continue
		}
		if !containsWorkspace(next, running) {
			log.Info("Removed a workspace", "workspace", running.name)
			changes++
		}
		running.stop()
	}

	if changes == 0 {
		return nil
	}

	m.router.replace(next)
	kcp.ObserveWorkspaceReload("reloaded")

	// Discovery and the APIServices describe the old set until this runs.
	if m.onChange != nil {
		m.onChange()
	}
	return nil
}

func containsWorkspace(workspaces []*workspace, wanted *workspace) bool {
	for _, candidate := range workspaces {
		if candidate == wanted {
			return true
		}
	}
	return false
}

// stop ends everything the workspace was running. The old one is stopped only
// after its replacement is serving, so there is no window where the groups it
// backs are unreachable.
func (w *workspace) stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
