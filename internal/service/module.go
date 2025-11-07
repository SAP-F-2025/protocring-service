package service

import (
	"go.uber.org/fx"
)

// Module exports all services for fx dependency injection
var Module = fx.Options(
	fx.Provide(
		NewHealthService,
		fx.Annotate(
			NewViolationService,
			fx.As(new(ViolationServiceInterface)),
		),
	),
)
