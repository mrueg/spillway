package apiserver

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/mrueg/spillway/pkg/kcp"
)

// testRestConfig is what the workspace was reached with.
func testRestConfig(t *testing.T, path string) *rest.Config {
	t.Helper()

	if path == "" {
		return &rest.Config{Host: "https://kcp.example:6443/clusters/root:spillway"}
	}
	config, err := kcp.RestConfig(path)
	if err != nil {
		t.Fatalf("loading the kubeconfig: %v", err)
	}
	return config
}

func writeKubeconfig(t *testing.T, path, server, token string) {
	t.Helper()

	content := `apiVersion: v1
kind: Config
clusters:
  - name: kcp
    cluster:
      server: ` + server + `
      insecure-skip-tls-verify: true
contexts:
  - name: kcp
    context:
      cluster: kcp
      user: kcp
current-context: kcp
users:
  - name: kcp
    user:
      token: ` + token + `
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the kubeconfig: %v", err)
	}
}

func testCredentials(t *testing.T) (*credentialSource, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "kcp.kubeconfig")
	writeKubeconfig(t, path, "https://kcp.example:6443/clusters/root:spillway", "first")

	source, err := newCredentialSource(path, testRestConfig(t, path), testBackend())
	if err != nil {
		t.Fatalf("newCredentialSource: %v", err)
	}
	return source, path
}

// A rotated token has to be picked up without a restart, because a restart
// drops every watch spillway is proxying.
func TestCredentialsReloadWhenTheFileChanges(t *testing.T) {
	source, path := testCredentials(t)

	before := source.current.Load()
	if changed, err := source.reload(); err != nil || changed {
		t.Fatalf("reload with no change: changed=%v err=%v", changed, err)
	}
	if source.current.Load() != before {
		t.Error("the transport was replaced although the file had not changed")
	}

	writeKubeconfig(t, path, "https://kcp.example:6443/clusters/root:spillway", "second")

	changed, err := source.reload()
	if err != nil {
		t.Fatalf("reload after the rotation: %v", err)
	}
	if !changed {
		t.Fatal("the rotated kubeconfig was not picked up")
	}
	if source.current.Load() == before {
		t.Error("the transport was not replaced")
	}
}

// The proxy resolved the server URL into the path it prepends to every request,
// so a kubeconfig that moves the workspace is reported rather than followed.
func TestCredentialsRefuseToMoveTheWorkspace(t *testing.T) {
	source, path := testCredentials(t)
	before := source.current.Load()

	writeKubeconfig(t, path, "https://elsewhere.example:6443/clusters/root:other", "second")

	changed, err := source.reload()
	if err == nil {
		t.Fatal("a kubeconfig pointing somewhere else was accepted")
	}
	if changed || source.current.Load() != before {
		t.Error("the transport was replaced for a workspace spillway cannot follow")
	}

	// And it does not complain about the same file forever.
	if _, err := source.reload(); err != nil {
		t.Errorf("the same unchanged file was reported again: %v", err)
	}
}

// Without a path there is nothing to watch: the credentials came from the
// in-cluster environment, where client-go re-reads the token itself.
func TestCredentialsWithoutAFileDoNothing(t *testing.T) {
	source, err := newCredentialSource("", testRestConfig(t, ""), testBackend())
	if err != nil {
		t.Fatalf("newCredentialSource: %v", err)
	}
	if changed, err := source.reload(); changed || err != nil {
		t.Errorf("reload without a path: changed=%v err=%v", changed, err)
	}
}
