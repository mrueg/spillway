// Package main is the entrypoint for the spillway extension API server.
package main

import (
	"os"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"

	"github.com/mrueg/spillway/pkg/cmd/server"
	"github.com/mrueg/spillway/pkg/version"
)

func main() {
	ctx := genericapiserver.SetupSignalContext()

	options := server.NewSpillwayServerOptions(os.Stdout, os.Stderr)
	cmd := server.NewCommandStartSpillway(ctx, options)
	cmd.Version = version.Version

	os.Exit(cli.Run(cmd))
}
