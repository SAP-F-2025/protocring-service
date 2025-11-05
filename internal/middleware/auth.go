package middleware

import (
	"github.com/casdoor/casdoor-go-sdk/casdoorsdk"
	"github.com/gin-gonic/gin"
	"net/http"
	"protocring-service/internal/config"
	"strings"
)

// CasdoorAuthMiddleware provides authentication using Casdoor SDK
type CasdoorAuthMiddleware struct {
	client *casdoorsdk.Client
	config config.CasdoorConfig
}

// NewCasdoorAuthMiddleware creates a new Casdoor authentication middleware
func NewCasdoorAuthMiddleware(cfg *config.Config) *CasdoorAuthMiddleware {
	client := casdoorsdk.NewClient(
		cfg.Casdoor.Endpoint,
		cfg.Casdoor.ClientID,
		cfg.Casdoor.ClientSecret,
		cfg.Casdoor.Cert,
		cfg.Casdoor.Application,
		cfg.Casdoor.Organization,
	)

	return &CasdoorAuthMiddleware{
		client: client,
		config: cfg.Casdoor,
	}
}

func (cam *CasdoorAuthMiddleware) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract token from header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":   "unauthorized",
				"message": "authorization header missing",
			})
			c.Abort()
			return
		}

		// Get token string
		tokenParts := strings.Split(authHeader, " ")
		if len(tokenParts) != 2 || strings.ToLower(tokenParts[0]) != "bearer" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":   "unauthorized",
				"message": "invalid authorization header format",
			})
			c.Abort()
			return
		}

		token := tokenParts[1]

		// Validate token with Casdoor
		user, err := cam.client.ParseJwtToken(token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":   "unauthorized",
				"message": "invalid token",
			})
			c.Abort()
			return
		}

		// Set user information in context
		c.Set("user_id", user.Name)
		c.Set("user_role", user.Roles)
		c.Set("user", user)
	}
}
