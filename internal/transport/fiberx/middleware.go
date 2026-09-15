package fiberx

import (
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/requestid"
	"go.uber.org/zap"

	"go-init/internal/base"
)

func errorHandler(log *zap.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		var fiberErr *fiber.Error
		if errorsAsFiber(err, &fiberErr) {
			return c.Status(fiberErr.Code).JSON(errorBody{
				Error:   strings.ToLower(strings.ReplaceAll(http.StatusText(fiberErr.Code), " ", "_")),
				Message: fiberErr.Message,
			})
		}
		appErr := base.AsAppError(err)
		if appErr.Code >= http.StatusInternalServerError {
			log.Error("http_error", zap.Error(err), zap.String("path", c.Path()))
		}
		return c.Status(appErr.Code).JSON(errorBody{
			Error:   appErr.Message,
			Message: appErr.Error(),
		})
	}
}

func errorsAsFiber(err error, target **fiber.Error) bool {
	fiberErr, ok := err.(*fiber.Error)
	if !ok || fiberErr == nil {
		return false
	}
	*target = fiberErr
	return true
}

func accessLog(log *zap.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		log.Info("http_request",
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", c.Response().StatusCode()),
			zap.Duration("duration", time.Since(start)),
			zap.String("request_id", requestid.FromContext(c)),
		)
		return err
	}
}

func corsHandler(opts Options) fiber.Handler {
	origins := splitCSV(opts.AllowOrigins)
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	methods := splitCSV(opts.AllowMethods)
	if len(methods) == 0 {
		methods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	}
	headers := splitCSV(opts.AllowHeaders)
	if len(headers) == 0 {
		headers = []string{"Origin", "X-Requested-With", "Content-Type", "Accept", "Authorization", "authentication"}
	}
	credentials := opts.AllowCredentials
	for _, origin := range origins {
		if origin == "*" {
			credentials = false
			break
		}
	}
	return cors.New(cors.Config{
		AllowOrigins:     origins,
		AllowMethods:     methods,
		AllowHeaders:     headers,
		AllowCredentials: credentials,
	})
}

func requestID() fiber.Handler {
	return requestid.New()
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
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
