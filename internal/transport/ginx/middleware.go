package ginx

import (
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"go-init/internal/base"
)

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		c.Header("X-Request-ID", id)
		c.Set("requestid", id)
		c.Next()
	}
}

func recoverJSON(log *zap.Logger) gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		log.Error("http_panic", zap.Any("panic", recovered), zap.String("path", c.Request.URL.Path))
		Error(c, base.InternalServerError(errors.Errorf("panic: %v", recovered)))
	})
}

func accessLog(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		requestID, _ := c.Get("requestid")
		id, _ := requestID.(string)
		log.Info("http_request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", time.Since(start)),
			zap.String("request_id", id),
		)
	}
}

func corsHandler(opts Options) gin.HandlerFunc {
	origins := []string{"*"}
	if opts.AllowOrigins != "" && opts.AllowOrigins != "*" {
		origins = splitCSV(opts.AllowOrigins)
	}
	methods := []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	if opts.AllowMethods != "" {
		methods = splitCSV(opts.AllowMethods)
	}
	headers := []string{"Origin", "X-Requested-With", "Content-Type", "Accept", "Authorization", "authentication"}
	if opts.AllowHeaders != "" {
		headers = splitCSV(opts.AllowHeaders)
	}
	credentials := opts.AllowCredentials && !(len(origins) == 1 && origins[0] == "*")
	return cors.New(cors.Config{
		AllowOrigins:     origins,
		AllowMethods:     methods,
		AllowHeaders:     headers,
		AllowCredentials: credentials,
		MaxAge:           12 * time.Hour,
	})
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
