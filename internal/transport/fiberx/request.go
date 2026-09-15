package fiberx

import (
	"github.com/gofiber/fiber/v3"
	"github.com/pkg/errors"

	"go-init/internal/base"
)

// Body binds JSON using Fiber v3 Bind. Path fields use the `uri` tag, query fields use `query`.
func Body(c fiber.Ctx, dst any) error {
	if err := c.Bind().Body(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid request body"))
	}
	return base.Validate(dst)
}

func Query(c fiber.Ctx, dst any) error {
	if err := c.Bind().Query(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid query"))
	}
	return base.Validate(dst)
}

func Params(c fiber.Ctx, dst any) error {
	if err := c.Bind().URI(dst); err != nil {
		return base.BadRequest(errors.Wrap(err, "invalid path params"))
	}
	return base.Validate(dst)
}

func Param(c fiber.Ctx, name string) string {
	return c.Params(name)
}
