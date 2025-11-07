package module

import (
	"protocring-service/internal/repository"
	"protocring-service/internal/repository/postgresql"

	"go.uber.org/fx"
)

// Module exports all repositories for fx dependency injection
var Module = fx.Options(
	fx.Provide(
		fx.Annotate(
			postgresql.NewViolationRepository,
			fx.As(new(repository.ViolationRepositoryInterface)),
		),
	),
)
