package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrueg/spillway/pkg/apiserver"
)

// testDefaults is what the flags would contribute to a workspace that does not
// override them.
func testDefaults() apiserver.WorkspaceConfig {
	return testOptions("unused.example.com").workspaceDefaults()
}

func writeWorkspaces(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "workspaces.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the workspaces file: %v", err)
	}
	return path
}

func TestLoadWorkspaces(t *testing.T) {
	path := writeWorkspaces(t, `
workspaces:
  - name: team-a
    kubeconfig: /etc/spillway/team-a.kubeconfig
    apiGroups: ["*.team-a.example.com", "shared.example.com"]
  - name: team-b
    kubeconfig: /etc/spillway/team-b.kubeconfig
    apiGroups: ["*.team-b.example.com"]
`)

	workspaces, err := loadWorkspaces(path, testDefaults())
	if err != nil {
		t.Fatalf("loadWorkspaces: %v", err)
	}
	if len(workspaces) != 2 {
		t.Fatalf("loaded %d workspaces, want 2", len(workspaces))
	}
	if workspaces[0].Name != "team-a" || workspaces[0].Kubeconfig != "/etc/spillway/team-a.kubeconfig" {
		t.Errorf("first workspace = %+v", workspaces[0])
	}
	if !workspaces[0].APIGroups.Matches("widgets.team-a.example.com") ||
		!workspaces[0].APIGroups.Matches("shared.example.com") {
		t.Error("the first workspace does not match the groups it was given")
	}
	if workspaces[0].APIGroups.Matches("widgets.team-b.example.com") {
		t.Error("the first workspace matches the second's groups")
	}
}

// Two workspaces naming one group outright is not a preference to resolve.
// There is one APIService for a group, pointing at one spillway, which has to
// know which workspace to ask.
func TestLoadWorkspacesRefusesAContestedGroup(t *testing.T) {
	path := writeWorkspaces(t, `
workspaces:
  - name: team-a
    kubeconfig: /a
    apiGroups: ["shared.example.com"]
  - name: team-b
    kubeconfig: /b
    apiGroups: ["shared.example.com"]
`)

	_, err := loadWorkspaces(path, testDefaults())
	if err == nil {
		t.Fatal("two workspaces claiming one group were accepted")
	}
	if !strings.Contains(err.Error(), "shared.example.com") {
		t.Errorf("error = %v, want it to name the contested group", err)
	}
}

func TestLoadWorkspacesRejectsIncompleteEntries(t *testing.T) {
	for name, content := range map[string]string{
		"no workspaces":  "workspaces: []",
		"no name":        "workspaces:\n  - kubeconfig: /a\n    apiGroups: [\"a.example.com\"]",
		"no kubeconfig":  "workspaces:\n  - name: a\n    apiGroups: [\"a.example.com\"]",
		"no groups":      "workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: []",
		"duplicate name": "workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: [\"a.example.com\"]\n  - name: a\n    kubeconfig: /b\n    apiGroups: [\"b.example.com\"]",
		"bad wildcard":   "workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: [\"*.k8s.io\"]",
		"unknown field":  "workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: [\"a.example.com\"]\n    workspace: root:a",
	} {
		if _, err := loadWorkspaces(writeWorkspaces(t, content), testDefaults()); err == nil {
			t.Errorf("%s: accepted, want an error", name)
		}
	}
}

// The flags are the single workspace form of the same thing, so asking for both
// is a configuration with two answers.
func TestValidateRefusesTheFileAndTheFlagsTogether(t *testing.T) {
	options := testOptions("widgets.example.com")
	options.WorkspacesFile = writeWorkspaces(t, "workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: [\"a.example.com\"]")

	err := options.Validate()
	if err == nil || !strings.Contains(err.Error(), "workspaces-file") {
		t.Errorf("Validate() = %v, want it to refuse the combination", err)
	}
}

// Without the file, the flags describe one workspace, which is what everything
// downstream now takes.
func TestWorkspacesFromTheFlags(t *testing.T) {
	options := testOptions("widgets.example.com")
	options.KCPKubeconfig = "/etc/spillway/kcp.kubeconfig"

	workspaces, err := options.workspaces()
	if err != nil {
		t.Fatalf("workspaces: %v", err)
	}
	if len(workspaces) != 1 {
		t.Fatalf("got %d workspaces, want 1", len(workspaces))
	}
	if workspaces[0].Kubeconfig != "/etc/spillway/kcp.kubeconfig" ||
		!workspaces[0].APIGroups.Matches("widgets.example.com") {
		t.Errorf("workspace = %+v", workspaces[0])
	}
}

// One kcp being slow is a fact about that kcp. A workspace can say so without
// retuning every other workspace.
func TestWorkspacesOverrideTheFlags(t *testing.T) {
	path := writeWorkspaces(t, `
workspaces:
  - name: fast
    kubeconfig: /fast
    apiGroups: ["fast.example.com"]
  - name: slow
    kubeconfig: /slow
    apiGroups: ["slow.example.com"]
    impersonateUsers: true
    requestTimeout: 90s
    retries: 5
    failureThreshold: 20
    circuitCooldown: 2m
`)

	workspaces, err := loadWorkspaces(path, testDefaults())
	if err != nil {
		t.Fatalf("loadWorkspaces: %v", err)
	}

	defaults := testDefaults()
	if workspaces[0].Backend != defaults.Backend || workspaces[0].ImpersonateUsers != defaults.ImpersonateUsers {
		t.Errorf("the workspace that overrode nothing did not inherit the flags: %+v", workspaces[0])
	}

	slow := workspaces[1]
	if !slow.ImpersonateUsers {
		t.Error("impersonateUsers did not take effect")
	}
	if slow.Backend.RequestTimeout != 90*time.Second {
		t.Errorf("requestTimeout = %s, want 90s", slow.Backend.RequestTimeout)
	}
	if slow.Backend.Retries != 5 || slow.Backend.FailureThreshold != 20 {
		t.Errorf("retries/threshold = %d/%d, want 5/20", slow.Backend.Retries, slow.Backend.FailureThreshold)
	}
	if slow.Backend.CircuitCooldown != 2*time.Minute {
		t.Errorf("circuitCooldown = %s, want 2m", slow.Backend.CircuitCooldown)
	}
}

// An overridden setting has to obey the same rules the flag does, or a file can
// configure something a flag would have refused.
func TestWorkspacesValidateTheirOverrides(t *testing.T) {
	for name, override := range map[string]string{
		"zero timeout":      "requestTimeout: 0s",
		"negative retries":  "retries: -1",
		"threshold of zero": "failureThreshold: 0",
		"zero cooldown":     "circuitCooldown: 0s",
	} {
		content := "workspaces:\n  - name: a\n    kubeconfig: /a\n    apiGroups: [\"a.example.com\"]\n    " + override
		if _, err := loadWorkspaces(writeWorkspaces(t, content), testDefaults()); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
