// Package kcp holds the client plumbing for talking to the kcp workspace that
// backs the offloaded APIs.
package kcp

import (
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// RestConfig builds a client configuration from a kubeconfig pointing at a kcp
// workspace. An empty path falls back to the usual in-cluster and KUBECONFIG
// resolution, which is what a spillway running inside kcp itself would use.
func RestConfig(kubeconfigPath string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kcp kubeconfig %q: %w", kubeconfigPath, err)
	}

	return config, nil
}
