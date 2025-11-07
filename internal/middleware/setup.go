package middleware

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func SetupMiddleware(engine *gin.Engine, logger *zap.Logger, casdoorAuth *CasdoorAuthMiddleware) {
	engine.Use(gin.Recovery())

	engine.Use(RequestIDMiddleware())

	engine.Use(Logger(logger))

	engine.Use(SecurityMiddleware())

	engine.Use(CORSMiddleware())

	engine.Use(casdoorAuth.AuthMiddleware())
}
