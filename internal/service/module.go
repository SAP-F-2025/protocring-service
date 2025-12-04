package service

import (
	"go.uber.org/fx"
)

// Module exports all services for fx dependency injection
var Module = fx.Options(
	fx.Provide(
		NewHealthService,
		fx.Annotate(
			NewViolationServiceFactory,
			fx.As(new(ViolationServiceInterface)),
		),
	),
)
