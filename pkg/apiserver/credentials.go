package apiserver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/mrueg/spillway/pkg/kcp"
)

// credentialSource holds the transport that carries spillway's identity to a
// workspace, and replaces it when the kubeconfig on disk changes.
//
// The credentials were read once at startup, so rotating them meant restarting
// -- and a restart is not free here: it drops every watch spillway is proxying
// and re-syncs discovery for every workspace. A Secret whose token is rotated
// on a schedule would have taken the whole bridge down on a timer.
//
// Only the credentialed part is replaced. The circuit breaker, the retries and
// the metrics wrap this, so a rotation does not reset what spillway knows about
// whether kcp is answering.
type credentialSource struct {
	// path is the kubeconfig to watch. Empty means the credentials came from
	// the in-cluster environment, where client-go re-reads the token file
	// itself and there is nothing here to do.
	path    string
	options BackendOptions

	// server is the URL the workspace was reached at when spillway started. A
	// kubeconfig that moves the workspace elsewhere is reported rather than
	// followed: the proxy resolved that URL into the path it prepends to every
	// request, and swapping it underneath would be a different bridge.
	server string

	mu     sync.Mutex
	digest [sha256.Size]byte

	current atomic.Pointer[credentialedTransport]
}

// credentialedTransport is a round tripper behind a pointer, so it can be
// swapped atomically.
type credentialedTransport struct {
	transport http.RoundTripper
}

// newCredentialSource builds the first transport and remembers what it was
// built from.
func newCredentialSource(path string, config *rest.Config, options BackendOptions) (*credentialSource, error) {
	source := &credentialSource{path: path, options: options, server: config.Host}

	transport, err := credentialedRoundTripper(config, options)
	if err != nil {
		return nil, err
	}
	source.current.Store(&credentialedTransport{transport: transport})

	if digest, err := fileDigest(path); err == nil {
		source.digest = digest
	}
	return source, nil
}

func (s *credentialSource) RoundTrip(req *http.Request) (*http.Response, error) {
	return s.current.Load().transport.RoundTrip(req)
}

// reload rebuilds the transport if the kubeconfig has changed, and reports
// whether it did.
func (s *credentialSource) reload() (bool, error) {
	if s.path == "" {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	digest, err := fileDigest(s.path)
	if err != nil {
		return false, fmt.Errorf("reading the kcp kubeconfig: %w", err)
	}
	if digest == s.digest {
		return false, nil
	}

	config, err := kcp.RestConfig(s.path)
	if err != nil {
		return false, fmt.Errorf("loading the kcp kubeconfig: %w", err)
	}
	if config.Host != s.server {
		// Taking the digest anyway: the file is what it is, and complaining
		// about the same change on every tick helps nobody.
		s.digest = digest
		return false, fmt.Errorf("the kubeconfig now points at %s rather than %s; spillway keeps using "+
			"the original, and moving a workspace needs a restart", config.Host, s.server)
	}

	transport, err := credentialedRoundTripper(config, s.options)
	if err != nil {
		return false, err
	}

	s.current.Store(&credentialedTransport{transport: transport})
	s.digest = digest
	return true, nil
}

// run reloads on a timer until ctx is done. A failure is logged and retried on
// the next tick: the credentials in hand still work until they expire, and
// giving up on them because the file was briefly unreadable would be worse.
func (s *credentialSource) run(ctx context.Context, period time.Duration, workspace string) {
	if s.path == "" || period <= 0 {
		return
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := s.reload()
			switch {
			case err != nil:
				klog.FromContext(ctx).Error(err, "Reloading the kcp credentials", "workspace", workspace)
				kcp.ObserveCredentialReload(workspace, "error")
			case changed:
				klog.FromContext(ctx).Info("Reloaded the kcp credentials", "workspace", workspace)
				kcp.ObserveCredentialReload(workspace, "reloaded")
			}
		}
	}
}

func fileDigest(path string) ([sha256.Size]byte, error) {
	if path == "" {
		return [sha256.Size]byte{}, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(content), nil
}
