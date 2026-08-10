package main

import (
	"github.com/0xm4s0ud/cloud-management-plane/internal/fxmodules"
	"go.uber.org/fx"
)

func main() {
	fx.New(
		fxmodules.ConfigModule,
		fxmodules.ObservabilityModule,
		fxmodules.RepositoryModule,
		fxmodules.ServiceModule,
		fxmodules.HTTPModule,
	).Run()
}
