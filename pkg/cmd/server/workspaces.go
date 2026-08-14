package server

import (
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/yaml"

	"github.com/mrueg/spillway/pkg/apiserver"
	"github.com/mrueg/spillway/pkg/kcp"
)

// WorkspacesFile is the shape of --workspaces-file.
//
// Flags cannot express this. A workspace is a kubeconfig and the groups it
// backs, and repeating a flag pair leaves the pairing implicit -- the third
// --kcp-kubeconfig belonging to the third --api-group only by position. A file
// says which groups come from where, in one place, and is what a Secret mounts
// anyway.
type WorkspacesFile struct {
	Workspaces []WorkspaceEntry `json:"workspaces"`
}

// WorkspaceEntry is one workspace.
type WorkspaceEntry struct {
	// Name identifies the workspace in logs, metrics and errors. It is not the
	// kcp workspace's own name, which spillway never sees: the workspace is
	// selected by the path in the kubeconfig's server URL.
	Name string `json:"name"`

	// Kubeconfig is the path to the credentials for it. Its server URL must
	// address the workspace, as --kcp-kubeconfig's does.
	Kubeconfig string `json:"kubeconfig"`

	// APIGroups are the groups this workspace backs: exact names, domain
	// wildcards, exclusions, or all three, in the form --api-group takes.
	APIGroups []string `json:"apiGroups"`

	// The rest override the flags for this workspace alone. They are pointers
	// so that "not set" is distinguishable from the zero value, which for a
	// timeout would mean no timeout at all.
	ImpersonateUsers *bool            `json:"impersonateUsers,omitempty"`
	RequestTimeout   *metav1.Duration `json:"requestTimeout,omitempty"`
	Retries          *int             `json:"retries,omitempty"`
	FailureThreshold *int             `json:"failureThreshold,omitempty"`
	CircuitCooldown  *metav1.Duration `json:"circuitCooldown,omitempty"`
}

// loadWorkspaces reads and validates the workspaces file, with anything a
// workspace does not set inherited from the flags.
func loadWorkspaces(path string, defaults apiserver.WorkspaceConfig) ([]apiserver.WorkspaceConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading the workspaces file: %w", err)
	}

	file := &WorkspacesFile{}
	if err := yaml.UnmarshalStrict(raw, file); err != nil {
		return nil, fmt.Errorf("parsing the workspaces file: %w", err)
	}
	if len(file.Workspaces) == 0 {
		return nil, fmt.Errorf("the workspaces file lists no workspaces")
	}

	names := sets.New[string]()
	// Exact names are checked across workspaces because two workspaces serving
	// one group is not a preference to resolve -- there is one APIService for
	// it, pointing at one spillway, which must know which workspace to ask.
	claimed := map[string]string{}

	configs := make([]apiserver.WorkspaceConfig, 0, len(file.Workspaces))
	for i, entry := range file.Workspaces {
		where := fmt.Sprintf("workspace %d", i)
		if entry.Name != "" {
			where = fmt.Sprintf("workspace %q", entry.Name)
		}

		switch {
		case entry.Name == "":
			return nil, fmt.Errorf("%s has no name", where)
		case names.Has(entry.Name):
			return nil, fmt.Errorf("workspace %q is listed twice", entry.Name)
		case entry.Kubeconfig == "":
			return nil, fmt.Errorf("%s has no kubeconfig", where)
		}
		names.Insert(entry.Name)

		matcher, err := kcp.ParseGroupMatcher(entry.APIGroups)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		for _, group := range matcher.Exact() {
			if owner, taken := claimed[group]; taken {
				return nil, fmt.Errorf("workspaces %q and %q both serve group %s; a group can come "+
					"from only one workspace", owner, entry.Name, group)
			}
			claimed[group] = entry.Name
		}

		config := apiserver.WorkspaceConfig{
			Name:             entry.Name,
			Kubeconfig:       entry.Kubeconfig,
			APIGroups:        matcher,
			Backend:          defaults.Backend,
			ImpersonateUsers: defaults.ImpersonateUsers,
		}
		if entry.ImpersonateUsers != nil {
			config.ImpersonateUsers = *entry.ImpersonateUsers
		}
		if entry.RequestTimeout != nil {
			config.Backend.RequestTimeout = entry.RequestTimeout.Duration
		}
		if entry.Retries != nil {
			config.Backend.Retries = *entry.Retries
		}
		if entry.FailureThreshold != nil {
			config.Backend.FailureThreshold = *entry.FailureThreshold
		}
		if entry.CircuitCooldown != nil {
			config.Backend.CircuitCooldown = entry.CircuitCooldown.Duration
		}
		if err := validateBackend(where, config.Backend); err != nil {
			return nil, err
		}

		configs = append(configs, config)
	}

	return configs, nil
}

// validateBackend applies the same rules to an overridden setting that the
// flags apply to the default, so a per workspace value cannot be something the
// flag would have refused.
func validateBackend(where string, backend apiserver.BackendOptions) error {
	switch {
	case backend.RequestTimeout <= 0:
		return fmt.Errorf("%s: requestTimeout must be positive, got %s", where, backend.RequestTimeout)
	case backend.Retries < 0:
		return fmt.Errorf("%s: retries must not be negative, got %d", where, backend.Retries)
	case backend.FailureThreshold < 1:
		return fmt.Errorf("%s: failureThreshold must be at least 1, got %d", where, backend.FailureThreshold)
	case backend.CircuitCooldown <= 0:
		return fmt.Errorf("%s: circuitCooldown must be positive, got %s", where, backend.CircuitCooldown)
	}
	return nil
}
