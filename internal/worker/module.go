package worker

import (
	"go.uber.org/fx"
)

// Module exports worker components for fx dependency injection
var Module = fx.Options(
	fx.Provide(NewWorkerManager),
)
